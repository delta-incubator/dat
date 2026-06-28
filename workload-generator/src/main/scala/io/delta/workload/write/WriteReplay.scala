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

import java.nio.file.{Files, Path}

import org.apache.spark.sql.{SaveMode, SparkSession}

import io.delta.workload.deltaharness.{CommitRemoveFile, CommitRequest, DeltaHarness}
import io.delta.workload.json.WriteSerde
import io.delta.workload.model._

/**
 * The write-spec replay engine: reconstruct a fresh table from a write spec's commits. The
 * counterpart to [[WriteSpecBuilder]] (build + replay of a WriteSpec sit together); the validator
 * dispatches to [[replayInto]].
 */
object WriteReplay {

  /** Replay a write spec's commits into a fresh table at `replayTablePath`. */
  def replayInto(
      spark: SparkSession, testDir: Path, writeSpecFile: Path, replayTablePath: Path): Unit =
    replayWriteInto(spark, testDir, writeSpecFile, replayTablePath)

  // ===========================================================================
  // Write-spec replay (reconstruct a fresh table from a write spec's commits)
  // ===========================================================================

  /** Replay a write spec's commits into a fresh table at `replayTablePath`. */
  private def replayWriteInto(
      spark: SparkSession, testDir: Path, writeSpecFile: Path, replayTablePath: Path): Unit = {
    val writeSpec = WriteSerde.readWriteSpec(writeSpecFile)
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

  // ===========================================================================
  // Per-commit replay
  // ===========================================================================

  private def replayCommit(
      spark: SparkSession, commit: WriteCommit, idx: Int,
      tableRef: String, tablePath: String, testDir: Path): Unit = commit match {
    case c: CreateTableCommit =>
      spark.sql(TableSql.createTable(tableRef, c.schema,
        c.partitionColumns.getOrElse(Nil), c.properties.getOrElse(Map.empty)))

    case c: ReplaceTableCommit =>
      // Always replace via AS SELECT: `CREATE OR REPLACE TABLE delta.`path` (cols)` is treated as
      // create-and-validate by Delta; the query path honors the new schema. Data-less = 0 rows.
      val select = c.dataFiles.filter(_.nonEmpty) match {
        case Some(files) =>
          val resolved = files.map(f => testDir.resolve(f).toAbsolutePath.toString)
          val src = if (resolved.size == 1) s"parquet.`${resolved.head}`"
            else s"parquet.`${testDir.resolve(SpecLayout.commitDataDir(idx)).toAbsolutePath}`"
          s"SELECT * FROM $src"
        case None =>
          val nulls = c.schema.fields
            .map(f => s"CAST(NULL AS ${f.dataType.sql}) AS `${f.name}`").mkString(", ")
          s"SELECT * FROM (SELECT $nulls) WHERE false"
      }
      spark.sql(TableSql.replaceTableAsSelect(tableRef, select,
        c.partitionColumns.getOrElse(Nil), c.properties.getOrElse(Map.empty)))

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

    case c: EvolveSchemaCommit => replayEvolveSchema(spark, c, tableRef)

    case c: LowLevelCommitOp => replayLowLevelCommit(spark, c, idx, tablePath, testDir)
  }

  /**
   * Replay a schema add/rename/drop via `ALTER TABLE` (mirroring the capture-side ops), so the
   * engine enforces column-mapping requirements: e.g. it rejects a rename/drop on a non-CM table
   * that would reinterpret physical columns by the new logical name. Each `EvolveSchemaCommit`
   * carries exactly one of add/rename/drop, so this emits exactly one commit.
   */
  private def replayEvolveSchema(
      spark: SparkSession, commit: EvolveSchemaCommit, tableRef: String): Unit = {
    commit.addColumns.foreach { cols =>
      spark.sql(s"ALTER TABLE $tableRef ADD COLUMNS (${cols.toDDL})")
    }
    commit.renameColumns.foreach { renames =>
      for ((oldName, newName) <- renames)
        spark.sql(s"ALTER TABLE $tableRef RENAME COLUMN $oldName TO $newName")
    }
    commit.dropColumns.foreach { drops =>
      if (drops.size == 1) spark.sql(s"ALTER TABLE $tableRef DROP COLUMN ${drops.head}")
      else spark.sql(s"ALTER TABLE $tableRef DROP COLUMNS (${drops.mkString(", ")})")
    }
  }

  /**
   * Replay a low-level commit: write each add's bundled logical Parquet through the engine
   * (via `req.addDataParquet`, so column-mapping/partition/stats are engine-handled), and tombstone
   * each remove's referenced prior add, resolved to this replay table's own paths at that version.
   */
  private def replayLowLevelCommit(
      spark: SparkSession, commit: LowLevelCommitOp, idx: Int,
      tablePath: String, testDir: Path): Unit = {
    val addDataParquet = commit.addFiles.getOrElse(Seq.empty)
      .map(af => testDir.resolve(af.dataFile).toAbsolutePath.toString)
    val removePaths = commit.removeFiles.getOrElse(Seq.empty)
      .flatMap(rf => SpecLayout.addPathsAt(java.nio.file.Paths.get(tablePath), rf.addedAtCommit))
    DeltaHarness.get.commit(spark, tablePath,
      CommitRequest(
        addDataParquet = addDataParquet,
        schemaJson = commit.schema.map(_.json),
        properties = commit.tableProperties,
        setTransaction = commit.txn,
        removeFiles = removePaths.map(p => CommitRemoveFile(p, dataChange = true)),
        addDomainMetadata = commit.addDomainMetadata.getOrElse(Seq.empty),
        removeDomainMetadata = commit.removeDomainMetadata.getOrElse(Seq.empty)))
  }

  /** Read the bundled Parquet (full rows, incl. partition columns) and Append it. */
  private def loadDataFiles(
      spark: SparkSession, files: Seq[String], tablePath: String, testDir: Path): Unit = {
    val resolved = files.map(testDir.resolve)
    val missing = resolved.filterNot(Files.exists(_))
    require(missing.isEmpty,
      s"Replay FAILED: bundled data file(s) missing: ${missing.mkString(", ")}")
    if (resolved.nonEmpty) {
      spark.read.parquet(resolved.map(_.toAbsolutePath.toString): _*)
        .write.format("delta").mode(SaveMode.Append).save(tablePath)
    }
  }
}
