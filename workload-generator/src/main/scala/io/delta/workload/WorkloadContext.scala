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

import java.nio.file.{Files, Path, Paths}

import scala.collection.{IterableOnce, mutable}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import io.delta.workload.deltaharness.{CommitRemoveFile, CommitRequest, DeltaHarness}
import io.delta.workload.log.{Action, CommitLog}

/**
 * Handle to a declared spec. Attach assertions that are checked after capture.
 * `T` is the typed spec class (e.g. [[ReadSpec]], [[SnapshotSpec]]).
 */
class SpecRef[T] private[workload] (
    private[workload] val config: HasAssertion[T]) {

  /**
   * Assert that the captured spec is an error (has `expectedError`, no `expected`).
   * Works for any spec type that follows the `expected`/`expectedError` convention.
   */
  def assertError(): SpecRef[T] = {
    config.assertion = Some { node =>
      require(node.has("expectedError") && !node.get("expectedError").isNull,
        s"Expected an error spec but got a success result")
    }
    this
  }

  /** Assert conditions on the captured spec using the typed case class. */
  def assert(check: T => Unit): SpecRef[T] = {
    val deserialize = config.deserialize
    config.assertion = Some { node =>
      check(deserialize(node))
    }
    this
  }
}

/**
 * Handle to a Delta table created via SQL. Used for declaring read specs
 * (read, snapshot) and table mutations (mutateTable, modifyCommitActions).
 */
class TableHandle private[workload] (
    private[workload] val tableName: String,
    private[workload] val sourcePath: Path,
    private[workload] val ctx: WorkloadContext) {

  /**
   * Get the commit timestamp for a specific version as a timestampAsOf-safe
   * string (yyyy-MM-dd HH:mm:ss.SSS in session TZ).
   *
   * Mirrors what Delta's time-travel resolution actually keys off:
   *   - ICT-enabled tables: `commitInfo.inCommitTimestamp` in the commit JSON.
   *   - Non-ICT tables:     the commit JSON file's mtime.
   *
   * DESCRIBE HISTORY and `commitInfo.timestamp` both look right at a glance
   * but diverge by a few milliseconds from the file mtime on some engines. Feeding
   * those back as `timestampAsOf` raises DELTA_TIMESTAMP_GREATER_THAN_COMMIT
   * / DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION.
   */
  def getTimestampForVersion(version: Long): String = {
    val padded = f"$version%020d"
    val commitPath = sourcePath.resolve(s"_delta_log/$padded.json")
    require(Files.exists(commitPath), s"No commit file for version $version: $commitPath")
    val ictOpt = scala.collection.JavaConverters
      .asScalaBufferConverter(Files.readAllLines(commitPath)).asScala.iterator
      .flatMap { line =>
        val node = JsonUtil.mapper.readTree(line)
        Option(node.get("commitInfo")).flatMap(ci => Option(ci.get("inCommitTimestamp")))
      }
      .find(_ => true)
      .map(_.asLong())
    val tsMillis = ictOpt.getOrElse(
      Files.getLastModifiedTime(commitPath).toMillis)
    val sessionTz = ctx.spark.conf.get("spark.sql.session.timeZone", "UTC")
    val fmt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    fmt.setTimeZone(java.util.TimeZone.getTimeZone(sessionTz))
    fmt.format(new java.util.Date(tsMillis))
  }
}

/**
 * Handle for structured write operations. Obtained from [[WorkloadContext.createTableOp]]
 * (which creates the table and records the create) or [[WorkloadContext.writeSpec]]
 * (which wraps an already-created [[TableHandle]]).
 *
 * Accepts write operations (insertOp, updateOp, deleteOp, ...) that both execute SQL
 * and record the commit. Call [[WorkloadContext.registerWriteSpec]] to finalize and
 * obtain a [[TableHandle]] for declaring read/snapshot specs.
 */
class WriteHandle private[workload] (private[workload] val table: TableHandle)

class WorkloadContext private[workload] (
    val spark: SparkSession,
    val workloadName: String,
    private[workload] val tags: Seq[String] = Seq.empty) {

  private val _createdTables = mutable.ArrayBuffer[String]()
  private[workload] val tableSpecs = mutable.ArrayBuffer[TableSpec]()

  /** Convert nullable java.lang.Long to Option[Long]. */
  private def opt(v: java.lang.Long): Option[Long] = Option(v).map(_.longValue())

  /** Wrap a possibly-empty collection: Some(it) when non-empty, else None. */
  private def opt[C <: Iterable[_]](c: C): Option[C] = if (c.nonEmpty) Some(c) else None

  // Per-table state: spec names must be unique within each table
  private val _tableSpecNames = mutable.HashMap[String, mutable.HashSet[String]]()

  // ---- SQL ----

  private val _createTableRegex =
    """(?i)(?:CREATE|REPLACE)\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?""".r

  /** Execute SQL. Tracks CREATE TABLE statements for cleanup. */
  def sql(statement: String): Unit = {
    _createTableRegex.findFirstMatchIn(statement).foreach { m =>
      _createdTables += m.group(1)
    }
    spark.sql(statement)
  }

  // ---- Table handles ----

  /** Register a managed Spark table for spec capture. */
  def registerTable(name: String): TableHandle = {
    val path = resolveTablePath(name)
    val handle = new TableHandle(name, path, this)
    ensureTableSpec(handle)
    handle
  }

  /** Register a table at a filesystem path for spec capture. */
  def registerTableFromPath(path: String): TableHandle = {
    val p = Paths.get(path)
    require(Files.exists(p.resolve("_delta_log")),
      s"No Delta table at path: $path")
    val name = p.getFileName.toString
    val handle = new TableHandle(name, p, this)
    ensureTableSpec(handle)
    handle
  }

  // ---- Spec declaration ----

  /**
   * Read spec. Name auto-generated from parameters.
   *
   * If `expectError` is set, capture asserts that Spark MUST throw when the
   * read is executed:
   *   - non-null + non-empty: the thrown error's code must match (after
   *     normalization). Capture fails if the operation succeeds or the code
   *     doesn't match.
   *   - non-null + empty string `""`: any error is accepted; capture fails
   *     only if the operation succeeds.
   *   - null (default): preserve legacy auto-detect behavior, recording
   *     whatever Spark does (success or error).
   */
  def readSpec(
      table: TableHandle,
      predicate: String = null,
      version: java.lang.Long = null,
      timestamp: String = null,
      columns: Seq[String] = null,
      name: String = null,
      expectError: String = null): SpecRef[ReadSpec] = {
    val specName = if (name != null) name else autoReadName(
      Option(predicate), opt(version),
      Option(timestamp), Option(columns))
    requireUnique(table, specName)
    val config = ReadSpecConfig(
      specName, Option(predicate), opt(version),
      Option(timestamp), Option(columns), Option(expectError))
    getTableSpec(table).readSpecs += config
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
      timestamp: String = null,
      expectError: String = null): SpecRef[SnapshotSpec] = {
    val config = SnapshotSpecConfig(opt(version), Option(timestamp), Option(expectError))
    getTableSpec(table).snapshotSpecs += config
    new SpecRef(config)
  }

  /**
   * Force Spark to write a checkpoint file for the given SQL table name.
   * Convenience wrapper around `DeltaHarness.openLog(...).checkpoint()`.
   */
  def forceCheckpoint(tableName: String): Unit = {
    val loc = spark.sql(s"DESCRIBE DETAIL `$tableName`").collect()(0).getAs[String]("location")
    DeltaHarness.get.openLog(spark, loc).checkpoint()
  }

  // ---- Write specs ----

  /** ` PARTITIONED BY (a, b)` clause, or empty when there are no partition columns. */
  private def partitionedByClause(partitionColumns: Seq[String]): String =
    if (partitionColumns.nonEmpty) s" PARTITIONED BY (${partitionColumns.mkString(", ")})" else ""

  /** ` TBLPROPERTIES ('k' = 'v', ...)` clause, or empty when there are no properties. */
  private def tblPropertiesClause(properties: Map[String, String]): String =
    if (properties.nonEmpty) {
      s" TBLPROPERTIES (${properties.map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")})"
    } else ""

  private val _writeBuilders = mutable.HashMap[String, WriteSpecBuilder]()

  private def getWriteBuilder(table: TableHandle): WriteSpecBuilder = {
    val key = s"${workloadName}_${table.tableName}"
    val builder = _writeBuilders.getOrElseUpdate(key, new WriteSpecBuilder())
    getTableSpec(table).writeBuilder = Some(builder)
    builder
  }

  /** Wrap an already-created [[TableHandle]] for structured write operations. */
  def writeSpec(table: TableHandle): WriteHandle = {
    getWriteBuilder(table)
    new WriteHandle(table)
  }

  /** Finalize write operations and return the [[TableHandle]] for read/snapshot specs. */
  def registerWriteSpec(w: WriteHandle): TableHandle = w.table

  /**
   * Create a table via SQL and record the create_table operation. `schema` is a SQL
   * DDL string (e.g. "id INT, name STRING NOT NULL"). Returns a [[WriteHandle]] for
   * further write operations.
   */
  def createTableOp(
      tableName: String,
      schema: String,
      properties: Map[String, String] = Map.empty,
      partitionColumns: Seq[String] = Seq.empty): WriteHandle = {
    val partitionClause = partitionedByClause(partitionColumns)
    val propsClause = tblPropertiesClause(properties)
    sql(s"CREATE TABLE $tableName ($schema) USING delta$partitionClause$propsClause")
    val t = registerTable(tableName)
    val b = getWriteBuilder(t)
    b.record(CreateTableCommit(
      schema = b.ddlToSchemaJson(schema),
      partitionColumns = opt(partitionColumns),
      properties = opt(properties)))
    new WriteHandle(t)
  }

  /**
   * Replace an existing table's schema/partitioning/properties and all data via
   * `CREATE OR REPLACE TABLE`, and record the replace_table operation. `schema` is a SQL DDL
   * string. With non-empty `rows` it is a replace-as-select (RTAS): a single commit that also
   * writes the rows. Operates on the table behind `w`; subsequent ops see the new schema.
   */
  def replaceTableOp(
      w: WriteHandle,
      schema: String,
      properties: Map[String, String] = Map.empty,
      partitionColumns: Seq[String] = Seq.empty,
      rows: IterableOnce[Map[String, Any]] = Seq.empty): Unit = {
    val rowSeq = rows.iterator.toSeq
    val partitionClause = partitionedByClause(partitionColumns)
    val propsClause = tblPropertiesClause(properties)
    if (rowSeq.nonEmpty) {
      // Single-commit replace-as-select: write the rows to a temp Parquet and RTAS from it, so
      // the resulting schema matches what the bundled spec Parquet will reproduce on replay.
      val parquet = DeltaHarness.get.writeRowsToTemp(spark, StructType.fromDDL(schema), rowSeq)
      try {
        sql(s"CREATE OR REPLACE TABLE ${w.table.tableName} USING delta$partitionClause" +
          s"$propsClause AS SELECT * FROM parquet.`${parquet.toAbsolutePath}`")
      } finally {
        FileUtils.deleteDirectory(parquet.getParent.toFile)
      }
    } else {
      sql(s"CREATE OR REPLACE TABLE ${w.table.tableName} ($schema) USING delta" +
        s"$partitionClause$propsClause")
    }
    val b = getWriteBuilder(w.table)
    b.record(ReplaceTableCommit(
      schema = b.ddlToSchemaJson(schema),
      partitionColumns = opt(partitionColumns),
      properties = opt(properties)), rowSeq)
  }

  /**
   * Insert `rows` (column -> value maps) and record the insert. An empty `rows` is a no-op:
   * it produces no commit, so recording it would both desync the commit-index/version mapping
   * and let the validator pass a spec with nothing to validate.
   */
  def insertOp(w: WriteHandle, rows: IterableOnce[Map[String, Any]]): Unit = {
    val rowSeq = rows.iterator.toSeq
    require(rowSeq.nonEmpty, "insertOp requires at least one row")
    // Drive the live insert from the SAME materialized Parquet the spec will bundle (via the
    // harness), so the captured table and the replayed table use one value encoder and
    // cannot diverge by type. The spec's own copy is re-materialized in buildSpec.
    val schema = DeltaHarness.get.schemaAt(spark, w.table.sourcePath.toString,
      version = None, includePartition = true)
    val parquet = DeltaHarness.get.writeRowsToTemp(spark, schema, rowSeq)
    try {
      sql(s"INSERT INTO ${w.table.tableName} SELECT * FROM parquet.`${parquet.toAbsolutePath}`")
    } finally {
      FileUtils.deleteDirectory(parquet.getParent.toFile)
    }
    getWriteBuilder(w.table).record(InsertCommit(), rowSeq)
  }

  /** Delete rows matching `predicate` and record the delete. */
  def deleteOp(w: WriteHandle, predicate: String): Unit = {
    sql(s"DELETE FROM ${w.table.tableName} WHERE $predicate")
    getWriteBuilder(w.table).record(DeleteCommit(predicate))
  }

  /** Update rows matching `predicate` with `set` (column -> expression) and record it. */
  def updateOp(w: WriteHandle, predicate: String, set: Map[String, String]): Unit = {
    val setClauses = set.map { case (k, v) => s"`$k` = $v" }.mkString(", ")
    sql(s"UPDATE ${w.table.tableName} SET $setClauses WHERE $predicate")
    getWriteBuilder(w.table).record(UpdateCommit(predicate, set))
  }


  /**
   * Execute a low-level commit of raw Delta actions against the live table and record it. `addFiles`
   * supply LOGICAL rows; the engine writes them (column-mapping/partition/stats handled) and the
   * actions are bundled with the raw `txn`/`addDomainMetadata`/`removeDomainMetadata`/schema/property
   * changes in one commit. `removeFiles` reference a prior low-level add by the ordinal that
   * [[commitOp]] returned (the commit/version that produced it). Returns this commit's ordinal so a
   * later `removeFiles` can target it.
   */
  def commitOp(
      w: WriteHandle,
      schemaDDL: Option[String] = None,
      tableProperties: Option[Map[String, String]] = None,
      txn: Option[AppTxn] = None,
      addFiles: Option[Seq[AddFileInput]] = None,
      removeFiles: Option[Seq[Int]] = None,
      addDomainMetadata: Option[Seq[AddDomainMetadata]] = None,
      removeDomainMetadata: Option[Seq[String]] = None): Int = {
    require(
      schemaDDL.isDefined || tableProperties.exists(_.nonEmpty) || txn.isDefined ||
        addFiles.exists(_.nonEmpty) || removeFiles.exists(_.nonEmpty) ||
        addDomainMetadata.exists(_.nonEmpty) || removeDomainMetadata.exists(_.nonEmpty),
      "commitOp requires at least one action (schema/properties/txn/addFiles/removeFiles/" +
        "domainMetadata); an empty commit would not advance the table version")
    val builder = getWriteBuilder(w.table)
    val idx = builder.nextOrdinal
    val livePath = w.table.sourcePath.toString
    val adds = addFiles.getOrElse(Seq.empty)

    // Materialize each add's logical rows to a temp Parquet for the live write.
    val schema = DeltaHarness.get.schemaAt(spark, livePath, version = None, includePartition = true)
    val tempParquet = adds.map(in => DeltaHarness.get.writeRowsToTemp(spark, schema, in.rows))
    // A remove targets all files added at the referenced commit (== that table version).
    val removePaths = removeFiles.getOrElse(Seq.empty)
      .flatMap(k => SpecLayout.addPathsAt(w.table.sourcePath, k))
    try {
      DeltaHarness.get.commitWithData(spark, livePath, tempParquet.map(_.toAbsolutePath.toString),
        CommitRequest(
          schemaJson = schemaDDL.map(ddl => StructType.fromDDL(ddl).json),
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
    // at the offending op, rather than deferring to buildSpec's whole-spec count check.
    val newVersion = DeltaHarness.get.openLog(spark, livePath).update().version
    require(newVersion == idx,
      s"commitOp produced version $newVersion but expected ordinal $idx; commit index can no " +
        "longer be used as the table version (ordinal-based removes would mis-resolve)")

    // Record the spec: adds become logical-Parquet pointers (materialized to data/ in buildSpec);
    // removes become ordinal references.
    val addActions = adds.zipWithIndex.map { case (in, i) =>
      AddFileAction(dataFile = SpecLayout.commitDataFile(idx, s"add_$i.parquet"), dataChange = in.dataChange)
    }
    builder.record(LowLevelCommitOp(
      schema = schemaDDL.map(builder.ddlToSchemaJson),
      tableProperties = tableProperties, txn = txn,
      addFiles = opt(addActions),
      removeFiles = removeFiles.map(_.map(k => RemoveFileAction(k))),
      addDomainMetadata = addDomainMetadata, removeDomainMetadata = removeDomainMetadata))
    builder.recordLowLevelRows(idx, adds.map(_.rows))
    idx
  }

  /** Add columns (SQL DDL) and record the schema evolution. */
  def addColumnsOp(w: WriteHandle, columnsDDL: String): Unit = {
    require(columnsDDL.nonEmpty, "addColumnsOp requires a non-empty DDL string")
    sql(s"ALTER TABLE ${w.table.tableName} ADD COLUMNS ($columnsDDL)")
    val b = getWriteBuilder(w.table)
    b.record(EvolveSchemaCommit(addColumns = b.addColumnsJson(columnsDDL)))
  }

  /** Rename a column and record the schema evolution. */
  def renameColumnOp(w: WriteHandle, oldName: String, newName: String): Unit = {
    sql(s"ALTER TABLE ${w.table.tableName} RENAME COLUMN $oldName TO $newName")
    getWriteBuilder(w.table).record(EvolveSchemaCommit(renameColumns = Some(Map(oldName -> newName))))
  }

  /** Drop columns and record the schema evolution. */
  def dropColumnsOp(w: WriteHandle, columns: Seq[String]): Unit = {
    require(columns.nonEmpty, "dropColumnsOp requires at least one column")
    if (columns.size == 1) {
      sql(s"ALTER TABLE ${w.table.tableName} DROP COLUMN ${columns.head}")
    } else {
      sql(s"ALTER TABLE ${w.table.tableName} DROP COLUMNS (${columns.mkString(", ")})")
    }
    getWriteBuilder(w.table).record(EvolveSchemaCommit(dropColumns = opt(columns)))
  }

  /** Set table properties and record the update_properties operation. */
  def setPropertiesOp(w: WriteHandle, props: Map[String, String]): Unit = {
    require(props.nonEmpty, "setPropertiesOp requires at least one property")
    val setClause = props.map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")
    sql(s"ALTER TABLE ${w.table.tableName} SET TBLPROPERTIES ($setClause)")
    getWriteBuilder(w.table).record(UpdatePropertiesCommit(set = Some(props)))
  }

  /** Unset table properties and record the update_properties operation. */
  def unsetPropertiesOp(w: WriteHandle, props: Seq[String]): Unit = {
    require(props.nonEmpty, "unsetPropertiesOp requires at least one property")
    sql(s"ALTER TABLE ${w.table.tableName} UNSET TBLPROPERTIES (${props.map(k => s"'$k'").mkString(", ")})")
    getWriteBuilder(w.table).record(UpdatePropertiesCommit(remove = Some(props)))
  }

  // ---- Table mutations (applied to copied table before spec capture) ----

  /** Mutate the copied table's filesystem before specs are captured. */
  def mutateTable(table: TableHandle)(mutation: Path => Unit): Unit = {
    getTableSpec(table).mutations += mutation
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

  // ---- Auto-naming ----

  private def autoReadName(
      predicate: Option[String],
      version: Option[Long],
      timestamp: Option[String],
      columns: Option[Seq[String]]): String = {
    val parts = mutable.ArrayBuffer[String]("read")
    version.foreach(v => parts += s"v$v")
    timestamp.foreach { ts =>
      parts += "ts_" + ts.replace(":", "-").replace(" ", "_").take(23)
    }
    predicate.foreach { p =>
      val simplified = p.toLowerCase
        .replaceAll(">=", "_gte_").replaceAll("<=", "_lte_")
        .replaceAll("<>", "_neq_").replaceAll("!=", "_neq_")
        .replaceAll(">", "_gt_").replaceAll("<", "_lt_")
        .replaceAll("=", "_eq_")
        .replaceAll("\\s+and\\s+", "_and_").replaceAll("\\s+or\\s+", "_or_")
        .replaceAll("\\s+is\\s+not\\s+null", "_is_not_null")
        .replaceAll("\\s+is\\s+null", "_is_null")
        .replaceAll("\\s+in\\s+", "_in_")
        .replaceAll("\\s+", "_")
        .replaceAll("[^a-z0-9_]", "")
        .replaceAll("_+", "_").stripPrefix("_").stripSuffix("_")
        .take(50)
      parts += simplified
    }
    columns.foreach { cols =>
      parts += "cols_" + cols.take(3).mkString("_")
      if (cols.size > 3) parts += s"plus${cols.size - 3}"
    }
    parts.mkString("_")
  }

  // ---- Internal ----

  private def ensureTableSpec(handle: TableHandle): Unit = {
    val outputName = s"${workloadName}_${handle.tableName}"
    if (!tableSpecs.exists(_.outputName == outputName)) {
      tableSpecs += new TableSpec(
        _outputName = outputName,
        description = s"$workloadName — ${handle.tableName}",
        tags = tags,
        sourcePath = handle.sourcePath
      )
      _tableSpecNames(outputName) = mutable.HashSet[String]()
    }
  }

  private def getTableSpec(handle: TableHandle): TableSpec = {
    val outputName = s"${workloadName}_${handle.tableName}"
    tableSpecs.find(_.outputName == outputName).getOrElse(
      throw new RuntimeException(s"No table spec for ${handle.tableName}. Call registerTable() first."))
  }

  private def requireUnique(handle: TableHandle, specName: String): Unit = {
    val outputName = s"${workloadName}_${handle.tableName}"
    val names = _tableSpecNames.getOrElseUpdate(outputName, mutable.HashSet[String]())
    require(names.add(specName),
      s"Duplicate spec name '$specName' for table '$outputName'")
  }

  private def resolveTablePath(tableName: String): Path = {
    try {
      val detail = spark.sql(s"DESCRIBE DETAIL $tableName").collect()
      require(detail.nonEmpty, s"DESCRIBE DETAIL returned no rows for '$tableName'")
      val location = detail(0).getAs[String]("location")
      require(location != null && location.nonEmpty, s"'$tableName' has no location")
      val path = if (location.startsWith("file:")) {
        Paths.get(new java.net.URI(location))
      } else Paths.get(location)
      require(Files.exists(path.resolve("_delta_log")),
        s"'$tableName' at $path has no _delta_log")
      path
    } catch {
      case e: org.apache.spark.sql.AnalysisException =>
        throw new RuntimeException(
          s"Table '$tableName' not found. Created tables: ${_createdTables.mkString(", ")}", e)
    }
  }

  private[workload] def cleanup(): Unit = {
    val warehouseDir = spark.conf.get("spark.sql.warehouse.dir", "")
    val errors = mutable.ArrayBuffer[String]()

    _createdTables.foreach { t =>
      // Get location before dropping (table may not exist in catalog, which is OK)
      val location = try {
        val tableId = spark.sessionState.catalog.getTableMetadata(
          org.apache.spark.sql.catalyst.TableIdentifier(t))
        Option(tableId.location).map(_.toString)
      } catch {
        case _: org.apache.spark.sql.catalyst.analysis.NoSuchTableException => None
        case _: org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException => None
      }

      // Drop from catalog
      spark.sql(s"DROP TABLE IF EXISTS `$t`")

      // Delete directory: try catalog location, then fall back to warehouse/tableName
      val pathsToDelete = location.toSeq.map { loc =>
        if (loc.startsWith("file:")) Paths.get(new java.net.URI(loc)) else Paths.get(loc)
      } ++ (if (warehouseDir.nonEmpty) {
        val base = if (warehouseDir.startsWith("file:"))
          Paths.get(new java.net.URI(warehouseDir)) else Paths.get(warehouseDir)
        Seq(base.resolve(t))
      } else Seq.empty)

      pathsToDelete.distinct.foreach { path =>
        if (Files.exists(path)) {
          org.apache.commons.io.FileUtils.deleteDirectory(path.toFile)
        }
      }
    }
  }

}

object WorkloadContext {
  private val _current = new scala.util.DynamicVariable[WorkloadContext](null)

  /** Get the current WorkloadContext. Throws if called outside a test body. */
  def current: WorkloadContext = {
    val ctx = _current.value
    require(ctx != null, "No active WorkloadContext. This method must be called inside a test body.")
    ctx
  }

  /** Execute a block with the given context as current. */
  def withContext[T](ctx: WorkloadContext)(body: => T): T = _current.withValue(ctx)(body)
}
