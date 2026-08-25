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

package io.delta.workload.write

import java.nio.file.Path

import scala.collection.mutable

import org.apache.spark.sql.SparkSession

import io.delta.workload.deltaharness.DeltaHarness
import io.delta.workload.json.JsonUtil
import io.delta.workload.model._

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
   * A recorded commit together with its capture-time logical rows. The serialized [[WriteCommit]]
   * carries only file paths; [[buildSpec]] materializes the rows to Parquet and writes the resulting
   * paths back into the commit. `rows` holds one row-set per data file the commit adds: high-level
   * insert / replace-as-select add a single file (one element); a low-level commit adds one row-set
   * per `AddFile`. Data-free ops carry no rows.
   */
  private case class WriteOpWithData(commit: WriteCommit, rows: Seq[Seq[Map[String, Any]]])

  private val recorded = mutable.ArrayBuffer[WriteOpWithData]()

  /** The ordinal (== table version) the next recorded commit will occupy. */
  def nextOrdinal: Int = recorded.size

  /**
   * Append a recorded commit. Pass the commit's logical `rows` (one row-set per data file it adds)
   * to materialize to Parquet in [[buildSpec]]; data-free ops pass nothing. The ordinal a commit
   * lands at (== the table version it produces) is read before recording via [[nextOrdinal]].
   */
  def record(commit: WriteCommit, rows: Seq[Seq[Map[String, Any]]] = Nil): Unit =
    recorded += WriteOpWithData(commit, rows)

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
        "(0.." + finalVersion + "); op index does not equal the table version")

    // Materialize the spec's data files as Parquet. High-level insert/replace-as-select rows are
    // written to `data/commit_<idx>/part-00000.parquet` (full schema incl. partition columns, so
    // replay can Append/RTAS). Low-level commits copy each add's already-written Parquet to
    // `data/commit_<idx>/<path>` (same in-table path).
    val enriched = recorded.zipWithIndex.map {
      case (WriteOpWithData(ins: InsertCommit, rows), idx) =>
        ins.copy(dataFiles = materializeRows(spark, tablePath.toString, outputDir, idx, rows))
      case (WriteOpWithData(rep: ReplaceTableCommit, rows), idx) =>
        rep.copy(dataFiles = materializeRows(spark, tablePath.toString, outputDir, idx, rows))
      case (WriteOpWithData(ll: LowLevelCommitOp, rows), idx) =>
        // Materialize each low-level add's logical rows to the `dataFile` path recorded in the
        // action (full schema incl. partition columns, so replay's writeFiles can partition/map).
        if (rows.nonEmpty) {
          val schema = DeltaHarness.get.schemaAt(spark, tablePath.toString, Some(idx.toLong))
          ll.addFiles.getOrElse(Seq.empty).zip(rows).foreach { case (af, r) =>
            DeltaHarness.get.writeRows(spark, schema, r, outputDir.resolve(af.dataFile))
          }
        }
        ll
      case (WriteOpWithData(other, _), _) => other
    }

    // `write` is just another spec: it lives in `specs/<name>_write.json` alongside read/snapshot
    // specs. The generator's baseline 'latest' read spec carries its final-state rows (validated
    // against the replay); the write spec's own check is basic (replay succeeds + commit-index ==
    // version).
    val writeName = s"${outputDir.getFileName.toString}_write"
    JsonUtil.writeSpec(
      outputDir.resolve("specs").resolve(s"$writeName.json"), WriteSpec(enriched.toSeq))
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
      val schema = DeltaHarness.get.schemaAt(spark, tablePath, Some(idx.toLong))
      val rel = SpecLayout.commitDataFile(idx, "part-00000.parquet")
      DeltaHarness.get.writeRows(spark, schema, r, outputDir.resolve(rel))
      Seq(rel)
    }
  }

}
