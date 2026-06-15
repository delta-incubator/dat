/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.workload

import java.nio.file.{Files, Path}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import io.delta.workload.deltaharness.{CommitRequest, DeltaHarness}

/**
 * Validates a write spec by:
 *   1. Replaying its commits against a fresh table using the data files in `data/`.
 *   2. Running every read/snapshot spec in `specs/` against the replayed table,
 *      comparing against `expected/`.
 *
 * CDF, checkpoint, CRC, domain-metadata, and appTxn specs are out of scope for the
 * write spec and are ignored here.
 */
object WriteSpecValidator {

  /**
   * Replay the write spec and validate read/snapshot specs against the result.
   *
   * @return warning/error messages (empty = all passed). A returned warning fails the
   *         generated workload; the orchestrator deletes the output so it auto-retries.
   */
  def validate(spark: SparkSession, outputDir: Path, specName: String): Seq[String] = {
    val writeSpecFile = outputDir.resolve("write_spec.json")
    if (!Files.exists(writeSpecFile)) return Seq.empty

    val warnings = mutable.ArrayBuffer[String]()
    val writeSpec = JsonUtil.readWriteSpec(writeSpecFile)
    if (writeSpec.commits.isEmpty) return Seq.empty

    // An op the reference replayer intentionally does not replay (e.g. `restore`, whose replayed
    // file paths differ from capture) means we skip REPLAY validation for the whole spec, but
    // this is not a failure. A data op recorded with no data files is a capture bug and IS a
    // failure. Check both before replaying anything.
    writeSpec.commits.collectFirst { case c if isReplayUnsupported(c) => c.operation }.foreach { op =>
      println(s"  Write spec validation skipping replay validation for $specName: " +
        s"'$op' not supported by reference replayer")
      return Seq.empty
    }
    writeSpec.commits.find(captureBugMissingDataFiles).foreach { c =>
      warnings += s"capture bug: '${c.operation}' recorded with no data files (not replayable)"
      return warnings.toSeq
    }

    val tempDir = Files.createTempDirectory(s"write_replay_$specName")
    val replayTablePath = tempDir.resolve("replay_table")

    try {
      val replayRef = s"delta.`${replayTablePath.toAbsolutePath}`"
      writeSpec.commits.foreach(c =>
        replayCommit(spark, c, replayRef, replayTablePath.toAbsolutePath.toString, outputDir))

      validateExpectedLatest(spark, replayTablePath, outputDir, specName, warnings)

      val specsDir = outputDir.resolve("specs")
      if (Files.isDirectory(specsDir)) {
        val specFiles = {
          val stream = Files.list(specsDir)
          try stream.iterator().asScala.filter(_.toString.endsWith(".json")).toSeq.sorted
          finally stream.close()
        }
        for (specFile <- specFiles) {
          val specJson = JsonUtil.mapper.readTree(Files.readAllBytes(specFile))
          val specType = Option(specJson.get("type")).map(_.asText()).getOrElse("")
          val specFileName = specFile.getFileName.toString.stripSuffix(".json")
          specType match {
            case "read" =>
              try {
                ReadCapture.validateFromSpec(spark, replayTablePath,
                  outputDir.resolve("expected").resolve(specFileName), specFile)
              } catch {
                case e: Throwable => warnings += s"read '$specFileName': ${e.getMessage}"
              }
            case "snapshot" =>
              validateSnapshotStructurally(spark, replayTablePath, specFile, specFileName,
                writeSpec, warnings)
            case _ => // out of scope (write/cdf/checkpoint/crc/...)
          }
        }
      }

      if (warnings.isEmpty) {
        println(s"  Write spec validation PASSED for $specName")
      }
    } finally {
      try org.apache.commons.io.FileUtils.deleteDirectory(tempDir.toFile)
      catch { case _: Throwable => }
    }

    warnings.toSeq
  }

  /** Compare the replayed latest snapshot against `expected/latest/` (rows + protocol/metadata). */
  private def validateExpectedLatest(
      spark: SparkSession,
      replayTablePath: Path,
      outputDir: Path,
      specName: String,
      warnings: mutable.ArrayBuffer[String]): Unit = {
    val latestDir = outputDir.resolve("expected").resolve("latest")
    val contentDir = latestDir.resolve("table_content")
    if (Files.exists(contentDir)) {
      val expected = JsonUtil.toRowMultiset(spark.read.parquet(contentDir.toString))
      val actual = JsonUtil.toRowMultiset(
        spark.read.format("delta").load(replayTablePath.toAbsolutePath.toString))
      try JsonUtil.assertMultisetsEqual(expected, actual, s"${specName}_latest")
      catch { case e: Throwable => warnings += e.getMessage }
    }
  }

  /**
   * Validate a snapshot spec against the replayed table structurally. A replay is an
   * independently created table, so its `id` and `createdTime` will never match the
   * original.
   *
   * `protocol` and `configuration` are compared with portable, engine-neutral semantics so a
   * spec validates against ANY conforming Delta writer, not just the one used to capture it:
   *   - `protocol`: capability check, not equality. The replay must meet every required floor
   *     (version >= expected, features superset of expected). A higher version or extra
   *     features on the replay are fine.
   *   - `configuration`: only the properties the write spec's own commits declared are checked
   *     (declared keys present + equal, removed keys absent). Engine-injected defaults are
   *     ignored.
   * `schemaString`, `partitionColumns`, and `format` are still compared exactly.
   */
  private def validateSnapshotStructurally(
      spark: SparkSession,
      replayTablePath: Path,
      specFile: Path,
      specFileName: String,
      writeSpec: WriteSpec,
      warnings: mutable.ArrayBuffer[String]): Unit = {
    val spec = JsonUtil.readSnapshotSpec(specFile)
    val (declaredKeys, removedKeys) = declaredConfiguration(writeSpec)
    spec.expected.foreach { exp =>
      val log = DeltaHarness.get.openLog(spark, replayTablePath.toString)
      val snapshot = (spec.version, spec.timestamp) match {
        case (Some(v), _) => log.getSnapshotAt(v)
        case _ => log.update()
      }
      val expProtocol = JsonUtil.mapper.valueToTree[com.fasterxml.jackson.databind.JsonNode](
        exp.protocol)
      val replayProtocol = JsonUtil.mapper.readTree(snapshot.protocolJson).get("protocol")
      protocolViolation(expProtocol, replayProtocol).foreach { reason =>
        warnings += s"snapshot '$specFileName': protocol mismatch ($reason)"
      }

      val expectedMeta = JsonUtil.mapper.valueToTree[com.fasterxml.jackson.databind.JsonNode](
        exp.metadata)
      val replayMeta = JsonUtil.mapper.readTree(snapshot.metadataJson).get("metaData")
      configurationViolation(expectedMeta, replayMeta, declaredKeys, removedKeys).foreach {
        reason => warnings += s"snapshot '$specFileName': metadata.configuration mismatch ($reason)"
      }
      for (field <- Seq("schemaString", "partitionColumns", "format")) {
        val expVal = canonicalField(expectedMeta, field)
        val repVal = canonicalField(replayMeta, field)
        if (expVal != repVal) warnings += s"snapshot '$specFileName': metadata.$field mismatch"
      }
    }
  }

  /**
   * Collect the configuration properties the write spec's commits explicitly declared.
   * @return (declared keys still in effect, keys explicitly removed). A key removed after being
   *         set is reported only as removed.
   */
  private def declaredConfiguration(writeSpec: WriteSpec): (Set[String], Set[String]) = {
    val declared = mutable.LinkedHashSet[String]()
    val removed = mutable.LinkedHashSet[String]()
    for (commit <- writeSpec.commits) commit.operation match {
      case "create_table" =>
        commit.properties.foreach(declared ++= _.keys)
      case "update_properties" =>
        commit.set.foreach { s => declared ++= s.keys; removed --= s.keys }
        commit.remove.foreach { r => removed ++= r; declared --= r }
      case _ => ()
    }
    (declared.toSet, removed.toSet)
  }

  /**
   * Check the replay protocol meets the expected one by capability: version floors and feature
   * supersets. Returns a human-readable reason on the first violation, or None if compatible.
   */
  private def protocolViolation(
      expected: com.fasterxml.jackson.databind.JsonNode,
      replay: com.fasterxml.jackson.databind.JsonNode): Option[String] = {
    def intField(node: com.fasterxml.jackson.databind.JsonNode, name: String): Int =
      if (node != null && node.has(name)) node.get(name).asInt() else 0
    def featureSet(
        node: com.fasterxml.jackson.databind.JsonNode, name: String): Set[String] =
      if (node != null && node.has(name) && node.get(name).isArray) {
        node.get(name).elements().asScala.map(_.asText()).toSet
      } else Set.empty

    val expReader = intField(expected, "minReaderVersion")
    val expWriter = intField(expected, "minWriterVersion")
    val repReader = intField(replay, "minReaderVersion")
    val repWriter = intField(replay, "minWriterVersion")
    val missingReaderFeatures =
      featureSet(expected, "readerFeatures") -- featureSet(replay, "readerFeatures")
    val missingWriterFeatures =
      featureSet(expected, "writerFeatures") -- featureSet(replay, "writerFeatures")

    if (repReader < expReader) Some(s"minReaderVersion $repReader < $expReader")
    else if (repWriter < expWriter) Some(s"minWriterVersion $repWriter < $expWriter")
    else if (missingReaderFeatures.nonEmpty)
      Some(s"missing readerFeatures ${missingReaderFeatures.mkString(",")}")
    else if (missingWriterFeatures.nonEmpty)
      Some(s"missing writerFeatures ${missingWriterFeatures.mkString(",")}")
    else None
  }

  /**
   * Check the replay configuration against only the author-declared keys: each declared key must
   * be present and equal on the replay, and each removed key must be absent. Engine-injected
   * default properties are ignored. Returns a reason on the first violation, or None.
   */
  private def configurationViolation(
      expectedMeta: com.fasterxml.jackson.databind.JsonNode,
      replayMeta: com.fasterxml.jackson.databind.JsonNode,
      declaredKeys: Set[String],
      removedKeys: Set[String]): Option[String] = {
    val expConfig = Option(expectedMeta.get("configuration")).filterNot(_.isNull)
    val repConfig = Option(replayMeta.get("configuration")).filterNot(_.isNull)
    def get(
        config: Option[com.fasterxml.jackson.databind.JsonNode],
        key: String): Option[String] =
      config.flatMap(c => Option(c.get(key))).filterNot(_.isNull).map(_.asText())

    val mismatch = declaredKeys.toSeq.sorted.collectFirst {
      case key if get(repConfig, key) != get(expConfig, key) =>
        s"key '$key' expected ${get(expConfig, key)}, got ${get(repConfig, key)}"
    }
    mismatch.orElse {
      removedKeys.toSeq.sorted.collectFirst {
        case key if get(repConfig, key).isDefined => s"removed key '$key' still present"
      }
    }
  }

  /** Canonicalize a single metadata field (or "null" if absent) for comparison. */
  private def canonicalField(
      node: com.fasterxml.jackson.databind.JsonNode, field: String): String =
    if (node.has(field) && !node.get(field).isNull) JsonUtil.canonicalJson(node.get(field))
    else "null"

  /** Ops the reference replayer intentionally does not replay; their presence skips replay. */
  private def isReplayUnsupported(commit: WriteCommit): Boolean =
    commit.operation == "restore"

  /**
   * A data op recorded with no/empty data files. Capture should always record the files an
   * `insert`/`create_table` wrote, so an empty record is a capture bug, not a missing feature.
   */
  private def captureBugMissingDataFiles(commit: WriteCommit): Boolean = commit.operation match {
    case "insert" => commit.dataFiles.forall(_.isEmpty)
    case _ => false
  }

  /** Replay a single commit against the replay table. */
  private def replayCommit(
      spark: SparkSession,
      commit: WriteCommit,
      tableRef: String,
      tablePath: String,
      outputDir: Path): Unit = {
    commit.operation match {
      case "create_table" =>
        commit.schema.foreach { schema =>
          var sql = s"CREATE TABLE $tableRef (${schemaToColDefs(schema)}) USING delta"
          commit.partitionColumns.filter(_.nonEmpty).foreach { parts =>
            sql += s" PARTITIONED BY (${parts.mkString(", ")})"
          }
          commit.properties.filter(_.nonEmpty).foreach { props =>
            sql += s" TBLPROPERTIES (${props.map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")})"
          }
          spark.sql(sql)
          commit.dataFiles.foreach(loadDataFiles(spark, _, tablePath, outputDir))
        }

      case "insert" =>
        commit.dataFiles.foreach(loadDataFiles(spark, _, tablePath, outputDir))

      case "delete" =>
        spark.sql(s"DELETE FROM $tableRef WHERE ${commit.predicate.getOrElse("true")}")

      case "update" =>
        commit.set.filter(_.nonEmpty).foreach { setMap =>
          val setClauses = setMap.map { case (k, v) => s"`$k` = $v" }.mkString(", ")
          spark.sql(s"UPDATE $tableRef SET $setClauses WHERE ${commit.predicate.getOrElse("true")}")
        }

      case "update_properties" =>
        val setClause = commit.set.getOrElse(Map.empty)
          .map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")
        if (setClause.nonEmpty) spark.sql(s"ALTER TABLE $tableRef SET TBLPROPERTIES ($setClause)")
        val unset = commit.remove.getOrElse(Seq.empty)
        if (unset.nonEmpty) {
          spark.sql(s"ALTER TABLE $tableRef UNSET TBLPROPERTIES (${unset.map(k => s"'$k'").mkString(", ")})")
        }

      case "evolve_schema" => replayEvolveSchema(spark, commit, tablePath)

      case "commit" => replayLowLevelCommit(spark, commit, tablePath, outputDir)

      case _ => ()
    }
  }

  /** Replay schema add/rename/drop via a single metadata-update commit. */
  private def replayEvolveSchema(spark: SparkSession, commit: WriteCommit, tablePath: String): Unit = {
    val log = DeltaHarness.get.openLog(spark, tablePath)
    val metaNode = Option(JsonUtil.mapper.readTree(log.update().metadataJson).get("metaData"))
      .getOrElse(throw new IllegalStateException(
        s"evolve_schema replay: no metaData action in current snapshot of $tablePath"))
    val schemaString = Option(metaNode.get("schemaString"))
      .getOrElse(throw new IllegalStateException(
        s"evolve_schema replay: metaData has no schemaString in $tablePath"))
    val currentSchema =
      DataType.fromJson(schemaString.asText()).asInstanceOf[StructType]
    var fields = currentSchema.fields.toBuffer

    commit.addColumns.foreach { cols =>
      cols.asInstanceOf[Seq[Map[String, Any]]].foreach { col =>
        val name = col("name").toString
        val nullable = col.get("nullable").forall(_.asInstanceOf[Boolean])
        fields += StructField(name, DataType.fromDDL(typeToSql(col("type"))), nullable)
      }
    }
    commit.renameColumns.foreach { renames =>
      for ((oldName, newName) <- renames) {
        val idx = fields.indexWhere(_.name == oldName)
        if (idx >= 0) fields(idx) = fields(idx).copy(name = newName)
      }
    }
    commit.dropColumns.foreach { drops =>
      fields = fields.filterNot(f => drops.contains(f.name))
    }

    DeltaHarness.get.commit(spark, tablePath,
      CommitRequest(schemaJson = Some(StructType(fields.toSeq).json)))
  }

  /**
   * Replay a low-level commit. `addFiles[].dataFile` was rewritten at capture to
   * `data/commit_N/<table-relative>`; we read from there and strip that prefix to recover
   * the table-relative path. Schema for a low-level commit is stored as Delta schema JSON,
   * so we round-trip it through `schemaToColDefs` to a DDL string for `LowLevelCommit`.
   */
  private def replayLowLevelCommit(
      spark: SparkSession,
      commit: WriteCommit,
      tablePath: String,
      outputDir: Path): Unit = {
    LowLevelCommit.apply(
      spark, tablePath,
      schemaDDL = commit.schema.map(schemaToColDefs),
      tableProperties = commit.tableProperties,
      txn = commit.txn,
      addFiles = commit.addFiles,
      removeFiles = commit.removeFiles,
      addDomainMetadata = commit.addDomainMetadata,
      removeDomainMetadata = commit.removeDomainMetadata,
      resolveDataFile = rel => outputDir.resolve(rel),
      assignPath = af => stripDataCommitPrefix(af.dataFile))
  }

  /** Strip the `data/commit_N/` prefix a captured commit prepends to its `addFiles` paths. */
  private def stripDataCommitPrefix(dataFile: String): String = {
    val p = java.nio.file.Paths.get(dataFile)
    if (p.getNameCount > 2) p.subpath(2, p.getNameCount).toString else dataFile
  }

  /**
   * Read parquet data files and append them to the replay table. Files for a partitioned
   * table are stored under `col=val/` directories within their `data/commit_N/` base; we
   * read with that base as `basePath` so Spark recovers the partition columns, otherwise
   * the appended DataFrame would be missing them.
   */
  private def loadDataFiles(
      spark: SparkSession,
      files: Seq[String],
      tablePath: String,
      outputDir: Path): Unit = {
    val resolved = files.map(outputDir.resolve).filter(Files.exists(_))
    if (resolved.isEmpty) return

    // All files in one insert commit share a `data/commit_N/` base.
    val basePath = outputDir.resolve(
      java.nio.file.Paths.get(files.head).subpath(0, 2)).toAbsolutePath
    val isPartitioned = resolved.exists(_.getParent != basePath)
    val df = if (isPartitioned) {
      spark.read.option("basePath", basePath.toString)
        .parquet(resolved.map(_.toAbsolutePath.toString): _*)
    } else {
      spark.read.parquet(resolved.map(_.toAbsolutePath.toString): _*)
    }
    df.write.format("delta").mode(SaveMode.Append).save(tablePath)
  }

  private def schemaToColDefs(schema: Any): String = {
    val schemaMap = schema.asInstanceOf[Map[String, Any]]
    val fields = schemaMap("fields").asInstanceOf[Seq[Map[String, Any]]]
    fields.map { f =>
      val name = f("name").toString
      val nullable = f.get("nullable").forall(_.asInstanceOf[Boolean])
      s"`$name` ${typeToSql(f("type"))}${if (!nullable) " NOT NULL" else ""}"
    }.mkString(", ")
  }

  private def typeToSql(typeValue: Any): String = typeValue match {
    case s: String => s match {
      case "integer" => "INT"
      case "long" => "BIGINT"
      case "short" => "SMALLINT"
      case "byte" => "TINYINT"
      case "float" => "FLOAT"
      case "double" => "DOUBLE"
      case "boolean" => "BOOLEAN"
      case "string" => "STRING"
      case "binary" => "BINARY"
      case "date" => "DATE"
      case "timestamp" => "TIMESTAMP"
      case "timestamp_ntz" => "TIMESTAMP_NTZ"
      case other => other.toUpperCase
    }
    case m: Map[_, _] =>
      val typeMap = m.asInstanceOf[Map[String, Any]]
      typeMap("type") match {
        case "struct" =>
          val fields = typeMap("fields").asInstanceOf[Seq[Map[String, Any]]]
          s"STRUCT<${fields.map(f => s"${f("name")}: ${typeToSql(f("type"))}").mkString(", ")}>"
        case "array" => s"ARRAY<${typeToSql(typeMap("elementType"))}>"
        case "map" => s"MAP<${typeToSql(typeMap("keyType"))}, ${typeToSql(typeMap("valueType"))}>"
        case other => other.toString.toUpperCase
      }
    case _ => typeValue.toString.toUpperCase
  }
}
