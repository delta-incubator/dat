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

import org.apache.spark.sql.SparkSession

import io.delta.workload.engine.SnapshotResolver
import io.delta.workload.log.{Action, CommitLog}
import io.delta.workload.model._

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

  /** Force Spark to write a checkpoint file for the given SQL table name. */
  def forceCheckpoint(tableName: String): Unit = current.forceCheckpoint(tableName)

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
