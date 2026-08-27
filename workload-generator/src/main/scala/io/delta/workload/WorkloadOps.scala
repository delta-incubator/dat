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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import io.delta.workload.deltaharness.{CommitRemoveFile, CommitRequest, DeltaHarness}
import io.delta.workload.engine.SnapshotResolver
import io.delta.workload.log.{Action, CommitLog}
import io.delta.workload.model._
import io.delta.workload.write.{SpecLayout, TableSql}

// ---------------------------------------------------------------------------
// WorkloadOps: DSL trait mixed into WorkloadSuite for clean syntax
// ---------------------------------------------------------------------------

/**
 * Provides DSL methods for workload generation. Mixed into WorkloadSuite
 * so that test bodies can call methods like `sql()`, `registerTable()`, `readSpec()`
 * directly without a context prefix.
 */
trait WorkloadOps {
  import WorkloadContext.current

  /** The active SparkSession. */
  def spark: SparkSession = current.spark

  /** Execute SQL. Tracks CREATE TABLE statements for cleanup. */
  def sql(statement: String): Unit = current.sql(statement)

  /** Register a managed Spark table for spec capture. */
  def registerTable(name: String): TableHandle = current.registerTable(name)

  /** Register a table at a filesystem path for spec capture. */
  def registerTableFromPath(path: String): TableHandle = current.registerTableFromPath(path)

  /**
   * Read spec. Name auto-generated from parameters.
   *
   * `expectError` (an [[ErrorExpectation]], default [[AutoDetect]]) asserts whether Spark must
   * throw when the read is executed:
   *   - [[ErrorCode]]: the thrown error's code must match (after normalization). Capture fails if
   *     the operation succeeds or the code doesn't match.
   *   - [[AnyError]]: any error is accepted; capture fails only if the operation succeeds.
   *   - [[AutoDetect]] (default): record whatever Spark does (success or error).
   */
  def readSpec(
      table: TableHandle,
      predicate: String = null,
      version: java.lang.Long = null,
      timestamp: java.time.Instant = null,
      columns: Option[Seq[String]] = None,
      name: Option[String] = None,
      expectError: ErrorExpectation = AutoDetect): SpecRef[ReadSpec] = {
    val ctx = WorkloadContext.current
    // Format the declared Instant to the timestampAsOf-safe wall-clock string here, at the DSL edge
    // (the only place a SparkSession is available), so the query carries a String everywhere.
    val timestampStr = Option(timestamp).map(ts => SnapshotResolver.formatTimestamp(ctx.spark, ts))
    val specName = name.getOrElse(ctx.autoReadName(
      Option(predicate), ctx.toOption(version), timestampStr, columns))
    ctx.requireUnique(table, specName)
    val config = ReadSpecConfig(
      specName,
      ReadQuery(ctx.toOption(version), timestampStr, Option(predicate), columns),
      expectError)
    ctx.getTableSpec(table).readSpecs += config
    new SpecRef(config)
  }

  /**
   * Snapshot spec.
   *
   * `expectError` has the same semantics as on [[readSpec]]: declaring it
   * asserts that Spark must throw, and capture fails loudly otherwise.
   */
  def snapshotSpec(
      table: TableHandle,
      version: java.lang.Long = null,
      timestamp: java.time.Instant = null,
      expectError: ErrorExpectation = AutoDetect): SpecRef[SnapshotSpec] = {
    val ctx = WorkloadContext.current
    val timestampStr = Option(timestamp).map(ts => SnapshotResolver.formatTimestamp(ctx.spark, ts))
    val config = SnapshotSpecConfig(SnapshotQuery(ctx.toOption(version), timestampStr), expectError)
    ctx.getTableSpec(table).snapshotSpecs += config
    new SpecRef(config)
  }

  /**
   * Change Data Feed spec over a version/timestamp range. Requires a start bound
   * (`startVersion` or `startTimestamp`).
   */
  def cdfSpec(
      table: TableHandle,
      startVersion: java.lang.Long = null,
      endVersion: java.lang.Long = null,
      startTimestamp: String = null,
      endTimestamp: String = null,
      predicate: String = null,
      columns: Seq[String] = null,
      name: String = null,
      expectError: String = null): SpecRef[CdfSpec] = {
    val ctx = WorkloadContext.current
    val specName = if (name != null) name else ctx.autoCdfName(
      ctx.toOption(startVersion), ctx.toOption(endVersion), Option(startTimestamp),
      Option(endTimestamp), Option(predicate), Option(columns))
    ctx.requireUnique(table, specName)
    val config = CdfSpecConfig(
      specName, ctx.toOption(startVersion), ctx.toOption(endVersion), Option(startTimestamp),
      Option(endTimestamp), Option(predicate), Option(columns), Option(expectError))
    ctx.getTableSpec(table).cdfSpecs += config
    new SpecRef(config)
  }

  /**
   * Checkpoint spec. Forces a checkpoint at `version` and asserts the reconstructed
   * protocol/metadata/txn/domain-metadata state, recorded inline in the spec JSON.
   * Targets classic V1 single-file checkpoints.
   */
  def checkpointSpec(
      table: TableHandle,
      version: Long,
      name: String = null): SpecRef[CheckpointSpec] = {
    val ctx = WorkloadContext.current
    val specName = if (name != null) name else s"checkpoint_v$version"
    ctx.requireUnique(table, specName)
    val config = CheckpointSpecConfig(specName, version)
    ctx.getTableSpec(table).checkpointSpecs += config
    new SpecRef(config)
  }

  /**
   * CRC (version-checksum) spec. Reads the `<version>.crc` file and asserts its core
   * aggregate fields (plus protocol and any present optional fields), recorded inline
   * in the spec JSON.
   */
  def crcSpec(
      table: TableHandle,
      version: Long,
      name: String = null): SpecRef[CrcSpec] = {
    val ctx = WorkloadContext.current
    val specName = if (name != null) name else s"crc_v$version"
    ctx.requireUnique(table, specName)
    val config = CrcSpecConfig(specName, version)
    ctx.getTableSpec(table).crcSpecs += config
    new SpecRef(config)
  }

  /** Force Spark to write a checkpoint file for the given SQL table name. */
  def forceCheckpoint(tableName: String): Unit = current.forceCheckpoint(tableName)

  // ---- Write specs ----

  /** Wrap an already-created [[TableHandle]] for structured write operations. */
  def beginWrite(table: TableHandle): WriteHandle = {
    val ctx = WorkloadContext.current
    ctx.getWriteBuilder(table)
    new WriteHandle(table)
  }

  /** Finalize write operations and return the [[TableHandle]] for read/snapshot specs. */
  def endWrite(w: WriteHandle): TableHandle = w.table

  /**
   * Create a table via SQL and record the create_table operation. Returns a [[WriteHandle]] for
   * further write operations.
   */
  def createTableOp(
      tableName: String,
      schema: StructType,
      properties: Map[String, String] = Map.empty,
      partitionColumns: Seq[String] = Seq.empty): WriteHandle = {
    val ctx = WorkloadContext.current
    ctx.sql(TableSql.createTable(tableName, schema, partitionColumns, properties))
    val t = ctx.registerTable(tableName)
    ctx.getWriteBuilder(t).record(CreateTableCommit(
      schema = schema,
      partitionColumns = ctx.toOption(partitionColumns),
      properties = ctx.toOption(properties)))
    new WriteHandle(t)
  }

  /**
   * Replace an existing table's schema/partitioning/properties and all data via
   * `CREATE OR REPLACE TABLE`, and record the replace_table operation. With non-empty `rows` it is
   * a replace-as-select (RTAS): a single commit that also writes the rows. Operates on the table
   * behind `w`; subsequent ops see the new schema.
   */
  def replaceTableOp(
      w: WriteHandle,
      schema: StructType,
      properties: Map[String, String] = Map.empty,
      partitionColumns: Seq[String] = Seq.empty,
      rows: Iterable[Map[String, Any]] = Seq.empty): Unit = {
    val ctx = WorkloadContext.current
    val rowSeq = rows.iterator.toSeq
    if (rowSeq.nonEmpty) {
      // Single-commit replace-as-select: write the rows to a temp Parquet and RTAS from it, so
      // the resulting schema matches what the bundled spec Parquet will reproduce on replay.
      val parquet = writeRowsToTemp(schema, rowSeq)
      try {
        ctx.sql(TableSql.replaceTableAsSelect(w.table.tableName,
          s"SELECT * FROM parquet.`${parquet.toAbsolutePath}`", partitionColumns, properties))
      } finally {
        FileUtils.deleteDirectory(parquet.getParent.toFile)
      }
    } else {
      ctx.sql(TableSql.createTable(w.table.tableName, schema, partitionColumns, properties,
        orReplace = true))
    }
    ctx.getWriteBuilder(w.table).record(ReplaceTableCommit(
      schema = schema,
      partitionColumns = ctx.toOption(partitionColumns),
      properties = ctx.toOption(properties)), Seq(rowSeq))
  }

  /** Write `rows` to a single Parquet file in a fresh temp dir; the caller deletes it after use. */
  private def writeRowsToTemp(schema: StructType, rows: Seq[Map[String, Any]]): Path = {
    val ctx = WorkloadContext.current
    val dest = Files.createTempDirectory("row-parquet").resolve("part-00000.parquet")
    DeltaHarness.get.writeRows(ctx.spark, schema, rows, dest)
    dest
  }

  /**
   * Insert `rows` (column -> value maps) and record the insert. An empty `rows` is a no-op:
   * it produces no commit, so recording it would both desync the commit-index/version mapping
   * and let the validator pass a spec with nothing to validate.
   */
  def insertOp(w: WriteHandle, rows: Iterable[Map[String, Any]]): Unit = {
    val ctx = WorkloadContext.current
    val rowSeq = rows.iterator.toSeq
    require(rowSeq.nonEmpty, "insertOp requires at least one row")
    // Drive the live insert from the SAME materialized Parquet the spec will bundle (via the
    // harness), so the captured table and the replayed table share one value encoder and agree by
    // type. The spec's own copy is re-materialized in buildSpec.
    val schema = DeltaHarness.get.schemaAt(ctx.spark, w.table.sourcePath.toString, version = None)
    val parquet = writeRowsToTemp(schema, rowSeq)
    try {
      ctx.sql(s"INSERT INTO ${w.table.tableName} SELECT * FROM parquet.`${parquet.toAbsolutePath}`")
    } finally {
      FileUtils.deleteDirectory(parquet.getParent.toFile)
    }
    ctx.getWriteBuilder(w.table).record(InsertCommit(), Seq(rowSeq))
  }

  /** Delete rows matching `predicate` and record the delete. */
  def deleteOp(w: WriteHandle, predicate: String): Unit = {
    val ctx = WorkloadContext.current
    ctx.sql(s"DELETE FROM ${w.table.tableName} WHERE $predicate")
    ctx.getWriteBuilder(w.table).record(DeleteCommit(predicate))
  }

  /** Update rows matching `predicate` with `set` (column -> expression) and record it. */
  def updateOp(w: WriteHandle, predicate: String, set: Map[String, String]): Unit = {
    val ctx = WorkloadContext.current
    val setClauses = set.map { case (k, v) => s"`$k` = $v" }.mkString(", ")
    ctx.sql(s"UPDATE ${w.table.tableName} SET $setClauses WHERE $predicate")
    ctx.getWriteBuilder(w.table).record(UpdateCommit(predicate, set))
  }

  /**
   * Execute a low-level commit of raw Delta actions against the live table and record it. `addFiles`
   * supply LOGICAL rows; the engine writes them (column-mapping/partition/stats handled) and the
   * actions are bundled with the raw `txn`/`addDomainMetadata`/`removeDomainMetadata`/schema/property
   * changes in one commit. `removeFiles` reference a prior low-level add by the [[CommitOrdinal]]
   * that [[commitOp]] returned (the commit/version that produced it). Returns this commit's
   * [[CommitOrdinal]] so a later `removeFiles` can target it.
   */
  def commitOp(
      w: WriteHandle,
      schema: Option[StructType] = None,
      tableProperties: Option[Map[String, String]] = None,
      txn: Option[AppTxn] = None,
      addFiles: Option[Seq[AddFileInput]] = None,
      removeFiles: Option[Seq[CommitOrdinal]] = None,
      addDomainMetadata: Option[Seq[AddDomainMetadata]] = None,
      removeDomainMetadata: Option[Seq[String]] = None): CommitOrdinal = {
    val ctx = WorkloadContext.current
    require(
      schema.isDefined || tableProperties.exists(_.nonEmpty) || txn.isDefined ||
        addFiles.exists(_.nonEmpty) || removeFiles.exists(_.nonEmpty) ||
        addDomainMetadata.exists(_.nonEmpty) || removeDomainMetadata.exists(_.nonEmpty),
      "commitOp requires at least one action (schema/properties/txn/addFiles/removeFiles/" +
        "domainMetadata); an empty commit would not advance the table version")
    val builder = ctx.getWriteBuilder(w.table)
    val idx = builder.nextOrdinal
    val livePath = w.table.sourcePath.toString
    val adds = addFiles.getOrElse(Seq.empty)

    // Materialize each add's logical rows to a temp Parquet for the live write.
    val liveSchema = DeltaHarness.get.schemaAt(ctx.spark, livePath, version = None)
    val tempParquet = adds.map(in => writeRowsToTemp(liveSchema, in.rows))
    // A remove targets all files added at the referenced commit (== that table version).
    val removePaths = removeFiles.getOrElse(Seq.empty)
      .flatMap(k => SpecLayout.addPathsAt(w.table.sourcePath, k.value))
    try {
      DeltaHarness.get.commit(ctx.spark, livePath,
        CommitRequest(
          addDataParquet = tempParquet.map(_.toAbsolutePath.toString),
          schemaJson = schema.map(_.json),
          properties = tableProperties,
          setTransaction = txn,
          removeFiles = removePaths.map(p => CommitRemoveFile(p, dataChange = true)),
          addDomainMetadata = addDomainMetadata.getOrElse(Seq.empty),
          removeDomainMetadata = removeDomainMetadata.getOrElse(Seq.empty)))
    } finally {
      tempParquet.foreach(p => FileUtils.deleteDirectory(p.getParent.toFile))
    }

    // Enforce the load-bearing commit-index == table-version invariant at the point it is
    // established: ordinal-based remove resolution (SpecLayout.addPathsAt) keys off it. Fail here,
    // at the offending op, so the error points at the cause rather than buildSpec's later count
    // check.
    val newVersion = DeltaHarness.get.openLog(ctx.spark, livePath).update().version
    require(newVersion == idx,
      s"commitOp produced version $newVersion but expected ordinal $idx; commit index can no " +
        "longer be used as the table version (ordinal-based removes would mis-resolve)")

    val addActions = adds.indices.map { i =>
      AddFileAction(dataFile = SpecLayout.commitDataFile(idx, s"add_$i.parquet"))
    }
    builder.record(LowLevelCommitOp(
      schema = schema,
      tableProperties = tableProperties, txn = txn,
      addFiles = ctx.toOption(addActions),
      removeFiles = removeFiles.map(_.map(k => RemoveFileAction(k.value))),
      addDomainMetadata = addDomainMetadata, removeDomainMetadata = removeDomainMetadata),
      adds.map(_.rows))
    CommitOrdinal(idx)
  }

  /** Add columns and record the schema evolution. */
  def addColumnsOp(w: WriteHandle, columns: StructType): Unit = {
    val ctx = WorkloadContext.current
    require(columns.nonEmpty, "addColumnsOp requires at least one column")
    ctx.sql(s"ALTER TABLE ${w.table.tableName} ADD COLUMNS (${columns.toDDL})")
    ctx.getWriteBuilder(w.table).record(EvolveSchemaCommit(addColumns = Some(columns)))
  }

  /** Rename a column and record the schema evolution. */
  def renameColumnOp(w: WriteHandle, oldName: String, newName: String): Unit = {
    val ctx = WorkloadContext.current
    ctx.sql(s"ALTER TABLE ${w.table.tableName} RENAME COLUMN $oldName TO $newName")
    ctx.getWriteBuilder(w.table).record(EvolveSchemaCommit(renameColumns = Some(Map(oldName -> newName))))
  }

  /** Drop columns and record the schema evolution. */
  def dropColumnsOp(w: WriteHandle, columns: Seq[String]): Unit = {
    val ctx = WorkloadContext.current
    require(columns.nonEmpty, "dropColumnsOp requires at least one column")
    if (columns.size == 1) {
      ctx.sql(s"ALTER TABLE ${w.table.tableName} DROP COLUMN ${columns.head}")
    } else {
      ctx.sql(s"ALTER TABLE ${w.table.tableName} DROP COLUMNS (${columns.mkString(", ")})")
    }
    ctx.getWriteBuilder(w.table).record(EvolveSchemaCommit(dropColumns = ctx.toOption(columns)))
  }

  /** Set table properties and record the update_properties operation. */
  def setPropertiesOp(w: WriteHandle, props: Map[String, String]): Unit = {
    val ctx = WorkloadContext.current
    require(props.nonEmpty, "setPropertiesOp requires at least one property")
    val setClause = props.map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")
    ctx.sql(s"ALTER TABLE ${w.table.tableName} SET TBLPROPERTIES ($setClause)")
    ctx.getWriteBuilder(w.table).record(UpdatePropertiesCommit(set = Some(props)))
  }

  /** Unset table properties and record the update_properties operation. */
  def unsetPropertiesOp(w: WriteHandle, props: Seq[String]): Unit = {
    val ctx = WorkloadContext.current
    require(props.nonEmpty, "unsetPropertiesOp requires at least one property")
    ctx.sql(s"ALTER TABLE ${w.table.tableName} UNSET TBLPROPERTIES (${props.map(k => s"'$k'").mkString(", ")})")
    ctx.getWriteBuilder(w.table).record(UpdatePropertiesCommit(remove = Some(props)))
  }

  // ---- Table mutations (applied to copied table before spec capture) ----

  /** Mutate the copied table's filesystem before specs are captured. */
  def mutateTable(table: TableHandle)(mutation: Path => Unit): Unit = {
    val ctx = WorkloadContext.current
    ctx.getTableSpec(table).mutations += mutation
  }

  /**
   * Modify the typed [[Action]]s of a specific commit version: filter, reorder, or `.copy` fields.
   * Unknown/malformed lines surface as [[io.delta.workload.log.RawAction]] and pass through
   * unchanged. For byte-level corruption (truncation, garbage), use [[mutateTable]] directly.
   */
  def modifyCommitActions(table: TableHandle, version: Long)(
      modifier: Seq[Action] => Seq[Action]): Unit =
    mutateTable(table) { tableDir =>
      if (Files.exists(CommitLog.commitFile(tableDir, version)))
        CommitLog.mutate(tableDir, version)(modifier)
    }
}
