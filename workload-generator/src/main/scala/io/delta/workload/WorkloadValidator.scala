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
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import io.delta.workload.deltaharness.{CommitRemoveFile, CommitRequest, DeltaHarness}

/** Result of validating a workload tree: per-spec pass/fail tallies + error details. */
case class ValidationResult(passed: Int, errors: Seq[String]) {
  def success: Boolean = errors.isEmpty
}

/**
 * The single validator for every spec type. Walks a generated workload tree (or one test dir)
 * and validates each captured spec under `specs/`, dispatching on the `type` field:
 *
 *   - `write`    — replay its `commits` into a fresh table, compare to `expected/<name>/`.
 *   - `read` / `snapshot` — if the spec carries a `writeSpec` pointer it is *write-derived*:
 *     reconstruct the table by replaying that sibling write spec, then validate **portably**
 *     (rows-only reads, capability-protocol, column-mapping-normalized schema, declared-config
 *     only). Otherwise it is *read-only*: validate against the captured `delta/` table exactly.
 *
 * Used both as the presubmit acceptance entry point ([[validateAll]]) and by the generator to
 * self-check a freshly generated test dir ([[validateTestDir]]).
 */
object WorkloadValidator {

  def validateAll(spark: SparkSession, workloadRoot: Path): ValidationResult = {
    require(Files.isDirectory(workloadRoot), s"Not a directory: $workloadRoot")
    val errors = mutable.ArrayBuffer[String]()
    var passed = 0
    for (testDir <- listChildren(workloadRoot).filter(Files.isDirectory(_))) {
      val r = validateTestDir(spark, testDir)
      passed += r.passed
      errors ++= r.errors
    }
    ValidationResult(passed, errors.toSeq)
  }

  /**
   * Validate every spec in one test dir. Write-derived read/snapshot specs and the write spec
   * share a single replayed table per write spec (memoized). Returns pass count + error details.
   */
  def validateTestDir(spark: SparkSession, testDir: Path): ValidationResult = {
    val specsDir = testDir.resolve("specs")
    if (!Files.isDirectory(specsDir)) return ValidationResult(0, Seq.empty)
    val deltaTable = testDir.resolve("delta")

    val errors = mutable.ArrayBuffer[String]()
    var passed = 0
    val replays = mutable.Map[String, Path]() // write-spec file name -> replayed table path
    val tempDirs = mutable.ArrayBuffer[Path]()
    def replayOf(writeSpecName: String): Path = replays.getOrElseUpdate(writeSpecName, {
      val td = Files.createTempDirectory("wl_replay")
      tempDirs += td
      val table = td.resolve("replay_table")
      replayWriteInto(spark, testDir, specsDir.resolve(writeSpecName), table)
      table
    })

    try {
      for (specFile <- listSpecs(specsDir)) {
        val name = specFile.getFileName.toString.stripSuffix(".json")
        val node = JsonUtil.mapper.readTree(Files.readAllBytes(specFile))
        val specType = Option(node.get("type")).map(_.asText()).getOrElse("")
        val writePtr = Option(node.get("writeSpec")).filterNot(_.isNull).map(_.asText())
        try {
          specType match {
            case "write" =>
              val w = mutable.ArrayBuffer[String]()
              validateExpectedLatest(spark, replayOf(specFile.getFileName.toString), testDir, name, w)
              if (w.isEmpty) passed += 1 else errors ++= w.map(m => s"${testDir.getFileName}/$name: $m")

            case "read" =>
              val expectedDir = testDir.resolve("expected").resolve(name)
              writePtr match {
                case Some(ws) => // write-derived: rows-only against the replayed table
                  ReadCapture.validateFromSpec(spark, replayOf(ws), expectedDir, specFile,
                    checkMetadata = false)
                  passed += 1
                case None => // read-only: exact, against the captured table
                  if (Files.isDirectory(deltaTable)) {
                    ReadCapture.validateFromSpec(spark, deltaTable, expectedDir, specFile)
                    passed += 1
                  } else {
                    errors += s"${testDir.getFileName}/$name: read-only spec but no captured " +
                      s"table at $deltaTable"
                  }
              }

            case "snapshot" =>
              writePtr match {
                case Some(ws) => // write-derived: structural, against the replayed table
                  val w = mutable.ArrayBuffer[String]()
                  validateSnapshotStructurally(spark, replayOf(ws), specFile, name,
                    JsonUtil.readWriteSpec(specsDir.resolve(ws)), w)
                  if (w.isEmpty) passed += 1
                  else errors ++= w.map(m => s"${testDir.getFileName}/$name: $m")
                case None =>
                  if (Files.isDirectory(deltaTable)) {
                    SnapshotCapture.validateFromSpec(spark, deltaTable, specFile)
                    passed += 1
                  } else {
                    errors += s"${testDir.getFileName}/$name: read-only spec but no captured " +
                      s"table at $deltaTable"
                  }
              }

            case other => throw new IllegalArgumentException(
              s"Unknown spec type '$other' in ${specFile.getFileName}")
          }
        } catch {
          case e: Throwable => errors += s"${testDir.getFileName}/$name: $e"
        }
      }
    } finally {
      tempDirs.foreach(td => try FileUtils.deleteDirectory(td.toFile) catch { case NonFatal(_) => })
    }
    ValidationResult(passed, errors.toSeq)
  }

  private def listSpecs(specsDir: Path): Seq[Path] = {
    val stream = Files.list(specsDir)
    try stream.iterator().asScala.filter(_.getFileName.toString.endsWith(".json")).toSeq.sorted
    finally stream.close()
  }

  private def listChildren(dir: Path): List[Path] = {
    val stream = Files.list(dir)
    try stream.iterator().asScala.toList finally stream.close()
  }

  // ===========================================================================
  // Write-spec replay (reconstruct a fresh table from a write spec's commits)
  // ===========================================================================

  /** Replay a write spec's commits into a fresh table at `replayTablePath`. */
  private def replayWriteInto(
      spark: SparkSession, testDir: Path, writeSpecFile: Path, replayTablePath: Path): Unit = {
    val writeSpec = JsonUtil.readWriteSpec(writeSpecFile)
    val ref = s"delta.`${replayTablePath.toAbsolutePath}`"
    val path = replayTablePath.toAbsolutePath.toString
    writeSpec.commits.zipWithIndex.foreach { case (c, idx) =>
      replayCommit(spark, c, idx, ref, path, testDir)
    }
    // Each commit must map to exactly one version, or ordinal-based remove resolution (which keys
    // off commit index == table version) silently mis-resolves. Fail loud if replay desynced.
    val finalVersion = DeltaHarness.get.openLog(spark, path).update().version
    require(finalVersion == writeSpec.commits.size - 1,
      s"replay produced ${finalVersion + 1} versions but the write spec has ${writeSpec.commits.size} " +
        "commits; commit index can no longer be used as the table version")
  }

  /** Compare the replayed table's rows against the write spec's `expected/<name>/table_content`. */
  private def validateExpectedLatest(
      spark: SparkSession, replayTablePath: Path, testDir: Path, writeName: String,
      warnings: mutable.ArrayBuffer[String]): Unit = {
    val contentDir = testDir.resolve("expected").resolve(writeName).resolve("table_content")
    // A write spec MUST ship its expected rows; a missing dir is a capture bug, not a pass.
    if (!Files.exists(contentDir)) {
      warnings += s"write spec '$writeName': expected/$writeName/table_content is missing"
      return
    }
    val expected = JsonUtil.toRowMultiset(spark.read.parquet(contentDir.toString))
    val actual = JsonUtil.toRowMultiset(
      spark.read.format("delta").load(replayTablePath.toAbsolutePath.toString))
    try JsonUtil.assertMultisetsEqual(expected, actual, s"${writeName}_latest")
    catch { case e: Throwable => warnings += e.getMessage }
  }

  /**
   * Validate a snapshot spec against a write-replayed (independently created) table. `protocol`
   * is a capability check (version floors + feature supersets), `configuration` checks only the
   * write spec's declared keys, and `schemaString` is compared with column-mapping ids stripped;
   * `partitionColumns`/`format` are compared exactly.
   */
  private def validateSnapshotStructurally(
      spark: SparkSession, replayTablePath: Path, specFile: Path, specFileName: String,
      writeSpec: WriteSpec, warnings: mutable.ArrayBuffer[String]): Unit = {
    val spec = JsonUtil.readSnapshotSpec(specFile)
    val (declaredKeys, removedKeys) = declaredConfiguration(writeSpec)
    spec.expected.foreach { exp =>
      val log = DeltaHarness.get.openLog(spark, replayTablePath.toString)
      val snapshot = (spec.version, spec.timestamp) match {
        case (Some(v), _) => log.getSnapshotAt(v)
        case _ => log.update()
      }
      val expProtocol = JsonUtil.mapper.valueToTree[JsonNode](exp.protocol)
      val replayProtocol = JsonUtil.mapper.readTree(snapshot.protocolJson).get("protocol")
      protocolViolation(expProtocol, replayProtocol).foreach { reason =>
        warnings += s"snapshot '$specFileName': protocol mismatch ($reason)"
      }
      val expectedMeta = JsonUtil.mapper.valueToTree[JsonNode](exp.metadata)
      val replayMeta = JsonUtil.mapper.readTree(snapshot.metadataJson).get("metaData")
      configurationViolation(expectedMeta, replayMeta, declaredKeys, removedKeys).foreach {
        reason => warnings += s"snapshot '$specFileName': metadata.configuration mismatch ($reason)"
      }
      if (normalizedSchema(expectedMeta) != normalizedSchema(replayMeta)) {
        warnings += s"snapshot '$specFileName': metadata.schemaString mismatch"
      }
      for (field <- Seq("partitionColumns", "format")) {
        if (canonicalField(expectedMeta, field) != canonicalField(replayMeta, field)) {
          warnings += s"snapshot '$specFileName': metadata.$field mismatch"
        }
      }
    }
  }

  // ===========================================================================
  // Portable comparison helpers (also unit-tested directly)
  // ===========================================================================

  /**
   * Canonicalize `metadata.schemaString`, stripping per-field column-mapping identifiers
   * (`delta.columnMapping.physicalName`/`.id`) which are minted per-table and so are not a
   * meaningful equality target across an independently-replayed table.
   */
  private[workload] def normalizedSchema(meta: JsonNode): String =
    Option(meta.get("schemaString")).filterNot(_.isNull) match {
      case None => "null"
      case Some(s) =>
        val tree = JsonUtil.mapper.readTree(s.asText())
        stripColumnMapping(tree)
        JsonUtil.canonicalJson(tree)
    }

  private def stripColumnMapping(node: JsonNode): Unit = node match {
    case obj: ObjectNode =>
      obj.get("metadata") match {
        case m: ObjectNode =>
          m.remove("delta.columnMapping.physicalName")
          m.remove("delta.columnMapping.id")
        case _ => ()
      }
      obj.elements().asScala.foreach(stripColumnMapping)
    case arr: ArrayNode => arr.elements().asScala.foreach(stripColumnMapping)
    case _ => ()
  }

  /**
   * The configuration keys the write spec's commits explicitly declared.
   * @return (declared keys still in effect, keys explicitly removed). `replace_table` resets both.
   */
  private[workload] def declaredConfiguration(writeSpec: WriteSpec): (Set[String], Set[String]) = {
    val declared = mutable.LinkedHashSet[String]()
    val removed = mutable.LinkedHashSet[String]()
    for (commit <- writeSpec.commits) commit match {
      case c: CreateTableCommit => c.properties.foreach(declared ++= _.keys)
      case c: ReplaceTableCommit =>
        declared.clear(); removed.clear()
        c.properties.foreach(declared ++= _.keys)
      case c: UpdatePropertiesCommit =>
        c.set.foreach { s => declared ++= s.keys; removed --= s.keys }
        c.remove.foreach { r => removed ++= r; declared --= r }
      case _ => ()
    }
    (declared.toSet, removed.toSet)
  }

  // Features implied by a protocol version when table features are NOT enumerated (reader < 3 /
  // writer < 7). Cumulative — a version implies its own and all lower versions' features.
  // columnMapping is the only reader-axis legacy feature; it is implied jointly (reader>=2 AND
  // writer>=5), so it lives in the writer map and is credited to the reader axis only when the
  // writer floor is also met (see effectiveReader) — a reader=2/writer<5 protocol does NOT imply it.
  private val writerImpliedFeatures: Map[Int, Set[String]] = Map(
    1 -> Set.empty,
    2 -> Set("appendOnly", "invariants"),
    3 -> Set("appendOnly", "invariants", "checkConstraints"),
    4 -> Set("appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns"),
    5 -> Set("appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns",
      "columnMapping"),
    6 -> Set("appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns",
      "columnMapping", "identityColumns"))

  /**
   * Capability check: version floors + feature supersets over EFFECTIVE feature sets (explicit
   * features unioned with those implied by the protocol version). Returns the first violation.
   */
  private[workload] def protocolViolation(expected: JsonNode, replay: JsonNode): Option[String] = {
    def intField(node: JsonNode, name: String): Int =
      if (node != null && node.has(name)) node.get(name).asInt() else 0
    def explicitFeatures(node: JsonNode, name: String): Set[String] =
      if (node != null && node.has(name) && node.get(name).isArray)
        node.get(name).elements().asScala.map(_.asText()).toSet
      else Set.empty
    def effectiveReader(node: JsonNode): Set[String] = {
      val r = intField(node, "minReaderVersion")
      val w = intField(node, "minWriterVersion")
      if (r >= 3) explicitFeatures(node, "readerFeatures")
      // Legacy: columnMapping is the only reader-implied feature, and only when reader>=2 AND
      // writer>=5 jointly (mirrors delta-spark's implicitlySupportedFeatures).
      else if (r >= 2 && w >= 5) Set("columnMapping")
      else Set.empty
    }
    def effectiveWriter(node: JsonNode): Set[String] = {
      val v = intField(node, "minWriterVersion")
      if (v >= 7) explicitFeatures(node, "writerFeatures")
      else writerImpliedFeatures.getOrElse(v, Set.empty)
    }

    val expReader = intField(expected, "minReaderVersion")
    val expWriter = intField(expected, "minWriterVersion")
    val repReader = intField(replay, "minReaderVersion")
    val repWriter = intField(replay, "minWriterVersion")
    val missingReaderFeatures = effectiveReader(expected) -- effectiveReader(replay)
    val missingWriterFeatures = effectiveWriter(expected) -- effectiveWriter(replay)

    if (repReader < expReader) Some(s"minReaderVersion $repReader < $expReader")
    else if (repWriter < expWriter) Some(s"minWriterVersion $repWriter < $expWriter")
    else if (missingReaderFeatures.nonEmpty)
      Some(s"missing readerFeatures ${missingReaderFeatures.mkString(",")}")
    else if (missingWriterFeatures.nonEmpty)
      Some(s"missing writerFeatures ${missingWriterFeatures.mkString(",")}")
    else None
  }

  /**
   * Check only the author-declared config keys: each declared key present + equal on the replay,
   * each removed key absent. Engine-injected defaults are ignored. Returns the first violation.
   */
  private[workload] def configurationViolation(
      expectedMeta: JsonNode, replayMeta: JsonNode,
      declaredKeys: Set[String], removedKeys: Set[String]): Option[String] = {
    val expConfig = Option(expectedMeta.get("configuration")).filterNot(_.isNull)
    val repConfig = Option(replayMeta.get("configuration")).filterNot(_.isNull)
    def get(config: Option[JsonNode], key: String): Option[String] =
      config.flatMap(c => Option(c.get(key))).filterNot(_.isNull).map(_.asText())

    declaredKeys.toSeq.sorted.collectFirst {
      case key if get(repConfig, key) != get(expConfig, key) =>
        s"key '$key' expected ${get(expConfig, key)}, got ${get(repConfig, key)}"
    }.orElse {
      removedKeys.toSeq.sorted.collectFirst {
        case key if get(repConfig, key).isDefined => s"removed key '$key' still present"
      }
    }
  }

  private def canonicalField(node: JsonNode, field: String): String =
    if (node.has(field) && !node.get(field).isNull) JsonUtil.canonicalJson(node.get(field))
    else "null"

  // ===========================================================================
  // Per-commit replay
  // ===========================================================================

  private def replayCommit(
      spark: SparkSession, commit: WriteCommit, idx: Int,
      tableRef: String, tablePath: String, testDir: Path): Unit = commit match {
    case c: CreateTableCommit =>
      var sql = s"CREATE TABLE $tableRef (${structOf(c.schema).toDDL}) USING delta"
      c.partitionColumns.filter(_.nonEmpty).foreach { p =>
        sql += s" PARTITIONED BY (${p.mkString(", ")})"
      }
      c.properties.filter(_.nonEmpty).foreach { props =>
        sql += s" TBLPROPERTIES (${props.map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")})"
      }
      spark.sql(sql)

    case c: ReplaceTableCommit =>
      val parts = c.partitionColumns.filter(_.nonEmpty)
        .map(p => s" PARTITIONED BY (${p.mkString(", ")})").getOrElse("")
      val props = c.properties.filter(_.nonEmpty)
        .map(m => s" TBLPROPERTIES (${m.map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")})")
        .getOrElse("")
      // Always replace via AS SELECT: `CREATE OR REPLACE TABLE delta.`path` (cols)` is treated as
      // create-and-validate by Delta; the query path honors the new schema. Data-less = 0 rows.
      val select = c.dataFiles.filter(_.nonEmpty) match {
        case Some(files) =>
          val resolved = files.map(f => testDir.resolve(f).toAbsolutePath.toString)
          val src = if (resolved.size == 1) s"parquet.`${resolved.head}`"
            else s"parquet.`${testDir.resolve(SpecLayout.commitDataDir(idx)).toAbsolutePath}`"
          s"SELECT * FROM $src"
        case None =>
          val nulls = structOf(c.schema).fields
            .map(f => s"CAST(NULL AS ${f.dataType.sql}) AS `${f.name}`").mkString(", ")
          s"SELECT * FROM (SELECT $nulls) WHERE false"
      }
      spark.sql(s"CREATE OR REPLACE TABLE $tableRef USING delta$parts$props AS $select")

    case c: InsertCommit =>
      c.dataFiles.foreach(loadDataFiles(spark, _, tablePath, testDir))

    case c: DeleteCommit =>
      spark.sql(s"DELETE FROM $tableRef WHERE ${c.predicate}")

    case c: UpdateCommit =>
      val setClauses = c.set.map { case (k, v) => s"`$k` = $v" }.mkString(", ")
      spark.sql(s"UPDATE $tableRef SET $setClauses WHERE ${c.predicate}")

    case c: UpdatePropertiesCommit =>
      c.set.filter(_.nonEmpty).foreach { s =>
        val setClause = s.map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")
        spark.sql(s"ALTER TABLE $tableRef SET TBLPROPERTIES ($setClause)")
      }
      c.remove.filter(_.nonEmpty).foreach { r =>
        spark.sql(s"ALTER TABLE $tableRef UNSET TBLPROPERTIES (${r.map(k => s"'$k'").mkString(", ")})")
      }

    case c: EvolveSchemaCommit => replayEvolveSchema(spark, c, tablePath)

    case c: LowLevelCommitOp => replayLowLevelCommit(spark, c, idx, tablePath, testDir)
  }

  /** Replay schema add/rename/drop via a single metadata-update commit. */
  private def replayEvolveSchema(
      spark: SparkSession, commit: EvolveSchemaCommit, tablePath: String): Unit = {
    val log = DeltaHarness.get.openLog(spark, tablePath)
    val metaNode = Option(JsonUtil.mapper.readTree(log.update().metadataJson).get("metaData"))
      .getOrElse(throw new IllegalStateException(
        s"evolve_schema replay: no metaData action in current snapshot of $tablePath"))
    val schemaString = Option(metaNode.get("schemaString"))
      .getOrElse(throw new IllegalStateException(
        s"evolve_schema replay: metaData has no schemaString in $tablePath"))
    val currentSchema = DataType.fromJson(schemaString.asText()).asInstanceOf[StructType]
    var fields = currentSchema.fields.toBuffer

    commit.addColumns.foreach { cols =>
      cols.asInstanceOf[Seq[Map[String, Any]]].foreach { col =>
        val name = col("name").toString
        val nullable = col.get("nullable").forall(_.asInstanceOf[Boolean])
        fields += StructField(
          name, DataType.fromJson(JsonUtil.mapper.writeValueAsString(col("type"))), nullable)
      }
    }
    commit.renameColumns.foreach { renames =>
      for ((oldName, newName) <- renames) {
        val i = fields.indexWhere(_.name == oldName)
        if (i >= 0) fields(i) = fields(i).copy(name = newName)
      }
    }
    commit.dropColumns.foreach { drops => fields = fields.filterNot(f => drops.contains(f.name)) }

    DeltaHarness.get.commit(spark, tablePath,
      CommitRequest(schemaJson = Some(StructType(fields.toSeq).json)))
  }

  /**
   * Replay a low-level commit: write each add's bundled logical Parquet through the engine
   * (`commitWithData` → column-mapping/partition/stats handled), and tombstone each remove's
   * referenced prior add — resolved to this replay table's own paths at that commit/version.
   */
  private def replayLowLevelCommit(
      spark: SparkSession, commit: LowLevelCommitOp, idx: Int,
      tablePath: String, testDir: Path): Unit = {
    val addDataParquet = commit.addFiles.getOrElse(Seq.empty)
      .map(af => testDir.resolve(af.dataFile).toAbsolutePath.toString)
    val removePaths = commit.removeFiles.getOrElse(Seq.empty)
      .flatMap(rf => SpecLayout.addPathsAt(java.nio.file.Paths.get(tablePath), rf.addedAtCommit))
    DeltaHarness.get.commitWithData(spark, tablePath, addDataParquet,
      CommitRequest(
        schemaJson = commit.schema.map(s => structOf(s).json),
        properties = commit.tableProperties,
        setTransaction = commit.txn,
        removeFiles = removePaths.map(p => CommitRemoveFile(p, dataChange = true)),
        addDomainMetadata = commit.addDomainMetadata.getOrElse(Seq.empty),
        removeDomainMetadata = commit.removeDomainMetadata.getOrElse(Seq.empty)))
  }

  /** Read the bundled Parquet (full rows, incl. partition columns) and Append it. */
  private def loadDataFiles(
      spark: SparkSession, files: Seq[String], tablePath: String, testDir: Path): Unit = {
    val resolved = files.map(testDir.resolve).filter(Files.exists(_)).map(_.toAbsolutePath.toString)
    if (resolved.nonEmpty) {
      spark.read.parquet(resolved: _*).write.format("delta").mode(SaveMode.Append).save(tablePath)
    }
  }

  /**
   * Reconstruct a Spark [[StructType]] from a stored Delta schema value. The spec stores schemas
   * as `StructType.json` deserialized to a Map (see `WriteSpecCapture.ddlToSchemaJson`), so this
   * re-serializes and parses it through Spark's own reader — `.toDDL`/`.sql`/`.json` on the result
   * are then exact and lossless (decimal precision, nested nullability, quoting all preserved).
   */
  private def structOf(schema: Any): StructType =
    DataType.fromJson(JsonUtil.mapper.writeValueAsString(schema)).asInstanceOf[StructType]
}
