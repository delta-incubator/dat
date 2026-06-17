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

import java.nio.file.Path

import scala.collection.IterableOnce

import org.apache.spark.sql.SparkSession

import io.delta.workload.log.Action

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
   * Read spec. Name auto-generated from parameters. See
   * [[WorkloadContext.readSpec]] for the semantics of `expectError`.
   */
  def readSpec(
      table: TableHandle,
      predicate: String = null,
      version: java.lang.Long = null,
      timestamp: String = null,
      columns: Seq[String] = null,
      name: String = null,
      expectError: String = null): SpecRef[ReadSpec] =
    current.readSpec(table, predicate, version, timestamp, columns, name, expectError)

  /**
   * Snapshot spec. See [[WorkloadContext.snapshotSpec]] for the semantics of
   * `expectError`.
   */
  def snapshotSpec(
      table: TableHandle,
      version: java.lang.Long = null,
      timestamp: String = null,
      expectError: String = null): SpecRef[SnapshotSpec] =
    current.snapshotSpec(table, version, timestamp, expectError)

  /** Force Spark to write a checkpoint file for the given SQL table name. */
  def forceCheckpoint(tableName: String): Unit = current.forceCheckpoint(tableName)

  // ---- Write specs ----

  /** Wrap an already-created [[TableHandle]] for structured write operations. */
  def writeSpec(table: TableHandle): WriteHandle = current.writeSpec(table)

  /** Finalize write operations and return the [[TableHandle]] for read/snapshot specs. */
  def registerWriteSpec(w: WriteHandle): TableHandle = current.registerWriteSpec(w)

  /** Create a table via SQL and record the create_table operation. */
  def createTableOp(
      tableName: String,
      schema: String,
      properties: Map[String, String] = Map.empty,
      partitionColumns: Seq[String] = Seq.empty): WriteHandle =
    current.createTableOp(tableName, schema, properties, partitionColumns)

  /** Replace an existing table (schema/partitioning/properties + data) and record it. */
  def replaceTableOp(
      w: WriteHandle,
      schema: String,
      properties: Map[String, String] = Map.empty,
      partitionColumns: Seq[String] = Seq.empty,
      rows: IterableOnce[Map[String, Any]] = Seq.empty): Unit =
    current.replaceTableOp(w, schema, properties, partitionColumns, rows)

  /** Insert rows and record the insert. */
  def insertOp(w: WriteHandle, rows: IterableOnce[Map[String, Any]]): Unit =
    current.insertOp(w, rows)

  /** Delete rows matching `predicate` and record the delete. */
  def deleteOp(w: WriteHandle, predicate: String): Unit = current.deleteOp(w, predicate)

  /** Update rows matching `predicate` with `set` and record the update. */
  def updateOp(w: WriteHandle, predicate: String, set: Map[String, String]): Unit =
    current.updateOp(w, predicate, set)


  /** Execute a low-level commit of raw Delta actions and record it. Returns the commit ordinal. */
  def commitOp(
      w: WriteHandle,
      schemaDDL: Option[String] = None,
      tableProperties: Option[Map[String, String]] = None,
      txn: Option[AppTxn] = None,
      addFiles: Option[Seq[AddFileInput]] = None,
      removeFiles: Option[Seq[Int]] = None,
      addDomainMetadata: Option[Seq[AddDomainMetadata]] = None,
      removeDomainMetadata: Option[Seq[String]] = None): Int =
    current.commitOp(w, schemaDDL, tableProperties, txn, addFiles, removeFiles,
      addDomainMetadata, removeDomainMetadata)

  /** Add columns (SQL DDL) and record the schema evolution. */
  def addColumnsOp(w: WriteHandle, columnsDDL: String): Unit =
    current.addColumnsOp(w, columnsDDL)

  /** Rename a column and record the schema evolution. */
  def renameColumnOp(w: WriteHandle, oldName: String, newName: String): Unit =
    current.renameColumnOp(w, oldName, newName)

  /** Drop columns and record the schema evolution. */
  def dropColumnsOp(w: WriteHandle, columns: Seq[String]): Unit =
    current.dropColumnsOp(w, columns)

  /** Set table properties and record the update_properties operation. */
  def setPropertiesOp(w: WriteHandle, props: Map[String, String]): Unit =
    current.setPropertiesOp(w, props)

  /** Unset table properties and record the update_properties operation. */
  def unsetPropertiesOp(w: WriteHandle, props: Seq[String]): Unit =
    current.unsetPropertiesOp(w, props)

  /** Mutate the copied table's filesystem before specs are captured. */
  def mutateTable(table: TableHandle)(mutation: Path => Unit): Unit =
    current.mutateTable(table)(mutation)

  /** Modify the typed actions of a specific commit version. */
  def modifyCommitActions(table: TableHandle, version: Long)(
      modifier: Seq[Action] => Seq[Action]): Unit =
    current.modifyCommitActions(table, version)(modifier)
}
