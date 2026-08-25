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

import scala.collection.mutable

import org.apache.spark.sql.SparkSession

import io.delta.workload.deltaharness.DeltaHarness
import io.delta.workload.write.WriteSpecBuilder

// ---------------------------------------------------------------------------
// WorkloadContext: the user's interface inside a workload() block
// ---------------------------------------------------------------------------

class WorkloadContext private[workload] (
    val spark: SparkSession,
    val workloadName: String,
    private[workload] val tags: Seq[String] = Seq.empty) {

  private val _createdTables = mutable.ArrayBuffer[String]()
  private[workload] val tableSpecs = mutable.ArrayBuffer[TableDecl]()

  private[workload] def toOption(v: java.lang.Long): Option[Long] = Option(v).map(_.longValue())

  /** None for an empty collection, so absent fields stay out of the serialized spec. */
  private[workload] def toOption[C <: Iterable[_]](c: C): Option[C] = if (c.nonEmpty) Some(c) else None

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

  /** Force Spark to write a checkpoint file for the given SQL table name. */
  def forceCheckpoint(tableName: String): Unit = {
    val loc = spark.sql(s"DESCRIBE DETAIL `$tableName`").collect()(0).getAs[String]("location")
    DeltaHarness.get.openLog(spark, loc).checkpoint()
  }

  // ---- Write specs ----

  private val _writeBuilders = mutable.HashMap[String, WriteSpecBuilder]()

  private[workload] def getWriteBuilder(table: TableHandle): WriteSpecBuilder = {
    val key = s"${workloadName}_${table.tableName}"
    val builder = _writeBuilders.getOrElseUpdate(key, new WriteSpecBuilder())
    getTableSpec(table).writeBuilder = Some(builder)
    builder
  }

  // ---- Auto-naming ----

  private[workload] def autoReadName(
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
    // No filters/time-travel/projection -> a read of the whole table.
    if (parts.length == 1) "read_all" else parts.mkString("_")
  }

  // ---- Internal ----

  private def ensureTableSpec(handle: TableHandle): Unit = {
    val outputName = s"${workloadName}_${handle.tableName}"
    if (!tableSpecs.exists(_.outputName == outputName)) {
      tableSpecs += new TableDecl(
        _outputName = outputName,
        description = s"$workloadName: ${handle.tableName}",
        tags = tags,
        sourcePath = handle.sourcePath
      )
      _tableSpecNames(outputName) = mutable.HashSet[String]()
    }
  }

  private[workload] def getTableSpec(handle: TableHandle): TableDecl = {
    val outputName = s"${workloadName}_${handle.tableName}"
    tableSpecs.find(_.outputName == outputName).getOrElse(
      throw new RuntimeException(s"No table spec for ${handle.tableName}. Call registerTable() first."))
  }

  private[workload] def requireUnique(handle: TableHandle, specName: String): Unit = {
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

    _createdTables.foreach { t =>
      // Read the location before dropping; a table absent from the catalog is fine.
      val location = try {
        val tableId = spark.sessionState.catalog.getTableMetadata(
          org.apache.spark.sql.catalyst.TableIdentifier(t))
        Option(tableId.location).map(_.toString)
      } catch {
        case _: org.apache.spark.sql.catalyst.analysis.NoSuchTableException => None
        case _: org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException => None
      }

      spark.sql(s"DROP TABLE IF EXISTS `$t`")

      // Delete the table directory: try the catalog location, then fall back to warehouse/tableName.
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
