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

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

import io.delta.workload.deltaharness.DeltaHarness

/**
 * Records structured write operations and serializes them as a `write` spec
 * (`specs/<name>_write.json`), the portable recipe a Delta writer replays to reconstruct a table.
 *
 * High-level operations carry SQL semantics (create_table, insert, update, ...); the low-level
 * `commit` carries raw Delta actions. Data-bearing ops (insert, replace-as-select, low-level adds)
 * record their LOGICAL rows, which `buildSpec` materializes to Parquet under `data/` so the spec
 * is self-sufficient for replay.
 */
class WriteSpecBuilder {

  /**
   * A recorded commit together with its capture-time logical rows. Rows are NOT part of the
   * serialized [[WriteCommit]] (which carries only file paths); [[buildSpec]] materializes them to
   * Parquet and writes the resulting paths back into the commit. `rows` holds one row-set per data
   * file the commit adds: high-level insert / replace-as-select add a single file (one element);
   * a low-level commit adds one row-set per `AddFile`. Data-free ops carry no rows.
   */
  private case class WriteOpWithData(commit: WriteCommit, rows: Seq[Seq[Map[String, Any]]])

  private val recorded = mutable.ArrayBuffer[WriteOpWithData]()

  /** The ordinal (== table version) the next recorded commit will occupy. */
  def nextOrdinal: Int = recorded.size

  /**
   * Append a recorded commit and return its ordinal (== the table version it produces). Pass the
   * commit's logical `rows` (one row-set per data file it adds) to materialize to Parquet in
   * [[buildSpec]]; data-free ops pass nothing.
   */
  def record(commit: WriteCommit, rows: Seq[Seq[Map[String, Any]]] = Nil): Int = {
    recorded += WriteOpWithData(commit, rows)
    recorded.size - 1
  }

  /** Parse `ADD COLUMNS` DDL into the spec's `addColumns` JSON (name/type/nullable per field). */
  private[workload] def addColumnsJson(ddl: String): Option[Any] =
    if (ddl.nonEmpty) {
      Some(StructType.fromDDL(ddl).fields.map { f =>
        val typeJson = JsonUtil.mapper.readValue(f.dataType.json, classOf[Any])
        Map[String, Any]("name" -> f.name, "type" -> typeJson, "nullable" -> f.nullable)
      }.toSeq)
    } else None

  /** Parse a SQL DDL string into Delta schema JSON using Spark's type parser. */
  private[workload] def ddlToSchemaJson(ddl: String): Any = {
    val st = StructType.fromDDL(ddl)
    JsonUtil.mapper.readValue(st.json, classOf[Any])
  }

  // === Serialization ===

  /**
   * Serialize recorded operations to `specs/<name>_write.json`, enriching the data-producing
   * commits (create_table, insert) with the actual data files they added, and writing
   * the expected final table state to `expected/<name>_write/`.
   *
   * New data files are copied to `outputDir/data/commit_<idx>/` and referenced via
   * relative paths in each commit's `dataFiles`, making the spec self-sufficient.
   */
  def buildSpec(spark: SparkSession, tablePath: Path, outputDir: Path): Unit = {
    val log = DeltaHarness.get.openLog(spark, tablePath.toString)

    // Commit index lines up with the table version it produced (commit 0 is the create at
    // version 0). Guard the coupling: each recorded op must map to exactly one version.
    val finalVersion = log.update().version
    require(recorded.size == finalVersion + 1,
      s"write spec records ${recorded.size} ops but table has ${finalVersion + 1} versions " +
        "(0.." + finalVersion + "); op index can no longer be used as the table version")

    // Materialize the spec's data files. The API took `rows`; the spec stores Parquet. High-level
    // insert/replace-as-select rows are written to `data/commit_<idx>/part-00000.parquet` (full
    // schema incl. partition columns, so replay can Append/RTAS). Low-level commits copy each
    // add's already-written Parquet to `data/commit_<idx>/<path>` (same in-table path).
    val enriched = recorded.zipWithIndex.map {
      case (WriteOpWithData(ins: InsertCommit, rows), idx) =>
        ins.copy(dataFiles = materializeRows(spark, tablePath.toString, outputDir, idx, rows))
      case (WriteOpWithData(rep: ReplaceTableCommit, rows), idx) =>
        rep.copy(dataFiles = materializeRows(spark, tablePath.toString, outputDir, idx, rows))
      case (WriteOpWithData(ll: LowLevelCommitOp, rows), idx) =>
        // Materialize each low-level add's logical rows to the `dataFile` path recorded in the
        // action (full schema incl. partition columns, so replay's writeFiles can partition/map).
        if (rows.nonEmpty) {
          val schema = DeltaHarness.get.schemaAt(spark, tablePath.toString, Some(idx.toLong),
            includePartition = true)
          ll.addFiles.getOrElse(Seq.empty).zip(rows).foreach { case (af, r) =>
            DeltaHarness.get.writeRows(spark, schema, r, outputDir.resolve(af.dataFile))
          }
        }
        ll
      case (WriteOpWithData(other, _), _) => other
    }

    // `write` is just another spec: it lives in `specs/<name>_write.json` alongside read/snapshot
    // specs, with its final-state under `expected/<name>_write/`.
    val writeName = s"${outputDir.getFileName.toString}_write"
    JsonUtil.writeSpec(
      outputDir.resolve("specs").resolve(s"$writeName.json"), WriteSpec(enriched.toSeq))
    writeExpectedLatest(spark, log, tablePath, outputDir.resolve("expected").resolve(writeName))
  }

  /**
   * Write the rows recorded for commit `idx` (if any) to `data/commit_<idx>/part-00000.parquet`
   * using the table's schema at that version, and return the spec-relative path. None if the op
   * recorded no rows.
   */
  private def materializeRows(
      spark: SparkSession, tablePath: String, outputDir: Path, idx: Int,
      rows: Seq[Seq[Map[String, Any]]]): Option[Seq[String]] = {
    rows.headOption.filter(_.nonEmpty).map { r =>
      val schema = DeltaHarness.get.schemaAt(spark, tablePath, Some(idx.toLong), includePartition = true)
      val rel = SpecLayout.commitDataFile(idx, "part-00000.parquet")
      DeltaHarness.get.writeRows(spark, schema, r, outputDir.resolve(rel))
      Seq(rel)
    }
  }

  /**
   * Capture the expected final table state under the write spec's `expected/<name>_write/`:
   *  - `table_content/`: Parquet of the latest snapshot's rows
   *  - `table_version_metadata.json`: protocol + metadata of the latest snapshot
   */
  private def writeExpectedLatest(
      spark: SparkSession,
      log: io.delta.workload.deltaharness.LogView,
      tablePath: Path,
      latestDir: Path): Unit = {
    Files.createDirectories(latestDir)

    val contentDir = latestDir.resolve("table_content")
    if (contentDir.toFile.exists()) {
      org.apache.commons.io.FileUtils.deleteDirectory(contentDir.toFile)
    }
    spark.read.format("delta").load(tablePath.toString)
      .write.mode(SaveMode.Overwrite).parquet(contentDir.toString)

    val snapshot = log.update()
    val protocol = JsonUtil.mapper.treeToValue(
      JsonUtil.mapper.readTree(snapshot.protocolJson).get("protocol"), classOf[Any])
    val metadata = JsonUtil.mapper.treeToValue(
      JsonUtil.mapper.readTree(snapshot.metadataJson).get("metaData"), classOf[Any])
    JsonUtil.writeSpec(
      latestDir.resolve("table_version_metadata.json"),
      SnapshotExpected(protocol, metadata))
  }
}
