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
              validateExpectedLatest(spark, replayOf(specFile.getFileName.toString), testDir, name)
              passed += 1

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
                case Some(ws) =>
                  // Write-derived: protocol/metadata are Spark's deterministic output and were
                  // validated at capture time. Standalone, we only confirm the write spec replays.
                  replayOf(ws)
                  passed += 1
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
      spark: SparkSession, replayTablePath: Path, testDir: Path, writeName: String): Unit = {
    val contentDir = testDir.resolve("expected").resolve(writeName).resolve("table_content")
    // A write spec MUST ship its expected rows; a missing dir is a capture bug.
    require(Files.exists(contentDir),
      s"write spec '$writeName': expected/$writeName/table_content is missing")
    val expected = JsonUtil.toRowMultiset(spark.read.parquet(contentDir.toString))
    val actual = JsonUtil.toRowMultiset(
      spark.read.format("delta").load(replayTablePath.toAbsolutePath.toString))
    JsonUtil.assertMultisetsEqual(expected, actual, s"${writeName}_latest")
  }

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
      // SET and UNSET replay as separate commits, so one UpdatePropertiesCommit must not do both
      // (it would advance the table by two versions and break commit index == version).
      require(!(c.set.exists(_.nonEmpty) && c.remove.exists(_.nonEmpty)),
        s"UpdatePropertiesCommit at commit $idx both sets and removes properties")
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
   * re-serializes and parses it through Spark's own reader. `.toDDL`/`.sql`/`.json` on the result
   * are then exact and lossless (decimal precision, nested nullability, quoting all preserved).
   */
  private def structOf(schema: Any): StructType =
    DataType.fromJson(JsonUtil.mapper.writeValueAsString(schema)).asInstanceOf[StructType]
}
