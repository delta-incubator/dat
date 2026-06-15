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

import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import scala.collection.mutable

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

import io.delta.workload.deltaharness.DeltaHarness

/**
 * Records structured write operations and serializes them as `write_spec.json`,
 * the portable recipe a Delta writer replays to reconstruct a table.
 *
 * High-level operations carry SQL semantics (create_table, insert, update, ...);
 * the low-level `commit` carries raw Delta actions. Data files produced by
 * insert/create_table commits are copied under `data/` so the spec is
 * self-sufficient for replay.
 */
class WriteSpecBuilder {

  private val commits = mutable.ArrayBuffer[WriteCommit]()

  // === High-level operations (SQL semantics) ===

  def recordCreateTable(schemaDDL: String, properties: Map[String, String],
      partitionColumns: Seq[String]): Unit = {
    commits += WriteCommit(
      operation = "create_table",
      schema = Some(ddlToSchemaJson(schemaDDL)),
      partitionColumns = if (partitionColumns.nonEmpty) Some(partitionColumns) else None,
      properties = if (properties.nonEmpty) Some(properties) else None)
  }

  def recordInsert(): Unit = commits += WriteCommit(operation = "insert")

  def recordDelete(predicate: String): Unit =
    commits += WriteCommit(operation = "delete", predicate = Some(predicate))

  def recordUpdate(predicate: String, set: Map[String, String]): Unit =
    commits += WriteCommit(operation = "update", predicate = Some(predicate), set = Some(set))

  def recordSetProperties(props: Map[String, String]): Unit =
    recordUpdateProperties(props, Seq.empty)

  def recordUnsetProperties(props: Seq[String]): Unit =
    recordUpdateProperties(Map.empty, props)

  def recordUpdateProperties(set: Map[String, String], unset: Seq[String]): Unit = {
    commits += WriteCommit(
      operation = "update_properties",
      set = if (set.nonEmpty) Some(set) else None,
      remove = if (unset.nonEmpty) Some(unset) else None)
  }

  def recordAddColumns(columnsDDL: String): Unit =
    recordEvolveSchema(columnsDDL, Map.empty, Seq.empty)

  def recordRenameColumn(oldName: String, newName: String): Unit =
    recordEvolveSchema("", Map(oldName -> newName), Seq.empty)

  def recordDropColumns(columns: Seq[String]): Unit =
    recordEvolveSchema("", Map.empty, columns)

  def recordEvolveSchema(
      addColumnsDDL: String,
      renameColumns: Map[String, String],
      dropColumns: Seq[String]): Unit = {
    val addCols = if (addColumnsDDL.nonEmpty) {
      val st = StructType.fromDDL(addColumnsDDL)
      Some(st.fields.map { f =>
        val typeJson = JsonUtil.mapper.readValue(f.dataType.json, classOf[Any])
        Map[String, Any]("name" -> f.name, "type" -> typeJson, "nullable" -> f.nullable)
      }.toSeq)
    } else None
    commits += WriteCommit(
      operation = "evolve_schema",
      addColumns = addCols,
      renameColumns = if (renameColumns.nonEmpty) Some(renameColumns) else None,
      dropColumns = if (dropColumns.nonEmpty) Some(dropColumns) else None)
  }

  def recordRestore(version: Long): Unit =
    commits += WriteCommit(operation = "restore", version = Some(version))

  // === Low-level operation (raw Delta actions) ===

  def recordCommit(
      schemaDDL: Option[String] = None,
      tableProperties: Option[Map[String, String]] = None,
      txn: Option[AppTxn] = None,
      addFiles: Option[Seq[AddFileAction]] = None,
      removeFiles: Option[Seq[RemoveFileAction]] = None,
      addDomainMetadata: Option[Seq[AddDomainMetadata]] = None,
      removeDomainMetadata: Option[Seq[String]] = None): Unit = {
    commits += WriteCommit(
      operation = "commit",
      schema = schemaDDL.map(ddlToSchemaJson),
      tableProperties = tableProperties,
      txn = txn,
      addFiles = addFiles,
      removeFiles = removeFiles,
      addDomainMetadata = addDomainMetadata,
      removeDomainMetadata = removeDomainMetadata)
  }

  // === Serialization ===

  /**
   * Serialize recorded operations to `write_spec.json`, enriching the data-producing
   * commits (create_table, insert) with the actual data files they added, and writing
   * the expected final table state to `expected/latest/`.
   *
   * New data files are copied to `outputDir/data/commit_<idx>/` and referenced via
   * relative paths in each commit's `dataFiles`, making the spec self-sufficient.
   */
  def buildSpec(spark: SparkSession, tablePath: Path, outputDir: Path): Unit = {
    val log = DeltaHarness.get.openLog(spark, tablePath.toString)
    val dataDir = outputDir.resolve("data")
    Files.createDirectories(dataDir)

    // Commit index lines up with the table version it produced (commit 0 is the create at
    // version 0). Guard the coupling: each recorded op must map to exactly one version.
    val finalVersion = log.update().version
    require(commits.size == finalVersion + 1,
      s"write spec records ${commits.size} ops but table has ${finalVersion + 1} versions " +
        "(0.." + finalVersion + "); op index can no longer be used as the table version")

    val highLevelDataOps = Set("insert", "create_table")

    // Enrich data-producing commits with the files they added so replay can reconstruct the
    // table from copied parquet. High-level ops carry these as `dataFiles`; low-level commits
    // already enumerate them in `addFiles`, whose `dataFile` paths we rewrite to the copies.
    val enriched = commits.zipWithIndex.map { case (commit, idx) =>
      if (highLevelDataOps.contains(commit.operation)) {
        val newFiles = newFilesAt(tablePath, idx)
        if (newFiles.nonEmpty) {
          val paths = newFiles.map(f => copyDataFile(tablePath, dataDir, idx, f))
          commit.copy(dataFiles = Some(paths))
        } else commit
      } else if (commit.operation == "commit") {
        commit.addFiles match {
          case Some(files) if files.nonEmpty =>
            val rewritten = files.map { af =>
              val copied = copyDataFile(tablePath, dataDir, idx, af.dataFile)
              af.copy(dataFile = copied)
            }
            commit.copy(addFiles = Some(rewritten))
          case _ => commit
        }
      } else commit
    }

    JsonUtil.writeSpec(outputDir.resolve("write_spec.json"), WriteSpec(enriched.toSeq))
    writeExpectedLatest(spark, log, tablePath, outputDir)
  }

  /**
   * Copy a table-relative data file into `data/commit_<idx>/`, preserving any `col=val/`
   * partition-directory prefix, and return the spec-relative path to the copy.
   */
  private def copyDataFile(tablePath: Path, dataDir: Path, idx: Int, relative: String): String = {
    val src = tablePath.resolve(relative)
    val dest = dataDir.resolve(s"commit_$idx").resolve(relative)
    Files.createDirectories(dest.getParent)
    if (Files.exists(src)) Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING)
    s"data/commit_$idx/$relative"
  }

  /**
   * Paths of the AddFile actions in commit `version`'s OWN log entry. Derived directly from
   * that commit's `add` actions rather than diffing adjacent snapshots' active-file sets,
   * which would misattribute files when a commit both adds and removes.
   */
  private def newFilesAt(tablePath: Path, version: Int): Seq[String] = {
    val commitFile = tablePath.resolve("_delta_log").resolve(f"$version%020d.json")
    if (!Files.exists(commitFile)) return Seq.empty
    val lines = new String(Files.readAllBytes(commitFile), "UTF-8")
      .split("\n").filter(_.trim.nonEmpty)
    lines.flatMap { line =>
      val node = JsonUtil.mapper.readTree(line)
      Option(node.get("add")).map(_.get("path").asText())
    }.toSeq
  }

  /**
   * Capture the expected final table state under `expected/latest/`:
   *  - `table_content/` — Parquet of the latest snapshot's rows
   *  - `table_version_metadata.json` — protocol + metadata of the latest snapshot
   */
  private def writeExpectedLatest(
      spark: SparkSession,
      log: io.delta.workload.deltaharness.LogView,
      tablePath: Path,
      outputDir: Path): Unit = {
    val latestDir = outputDir.resolve("expected").resolve("latest")
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

  /** Parse a SQL DDL string into Delta schema JSON using Spark's type parser. */
  private def ddlToSchemaJson(ddl: String): Any = {
    val st = StructType.fromDDL(ddl)
    JsonUtil.mapper.readValue(st.json, classOf[Any])
  }
}
