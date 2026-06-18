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

import scala.collection.JavaConverters._

import com.fasterxml.jackson.annotation._
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import io.delta.workload.deltaharness.{LogView, SnapshotView}
import io.delta.workload.log.{AddFile, CommitLog}

// =============================================================================
// Spec Expected types - success data or error info
// =============================================================================

/** Common error type for all specs. */
@JsonPropertyOrder(Array("errorCode", "errorMessage"))
case class SpecError(errorCode: String, errorMessage: String)

/** Read success data. */
@JsonPropertyOrder(Array("rowCount", "fileCount", "filesSkipped"))
case class ReadExpected(rowCount: Long, fileCount: Int, filesSkipped: Long)

/** Snapshot success data. */
@JsonPropertyOrder(Array("protocol", "metadata"))
case class SnapshotExpected(protocol: Any, metadata: Any)

// =============================================================================
// Spec case classes
// =============================================================================

@JsonPropertyOrder(Array("type", "writeSpec", "version", "timestamp", "predicate", "columns",
  "expected", "expectedError"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class ReadSpec(
    writeSpec: Option[String] = None,
    version: Option[Long] = None,
    timestamp: Option[String] = None,
    predicate: Option[String] = None,
    columns: Option[Seq[String]] = None,
    expected: Option[ReadExpected] = None,
    expectedError: Option[SpecError] = None) {
  val `type`: String = "read"
}

@JsonPropertyOrder(Array("type", "writeSpec", "version", "timestamp", "expected", "expectedError"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class SnapshotSpec(
    writeSpec: Option[String] = None,
    version: Option[Long] = None,
    timestamp: Option[String] = None,
    expected: Option[SnapshotExpected] = None,
    expectedError: Option[SpecError] = None) {
  val `type`: String = "snapshot"
}

// =============================================================================
// TableInfo case classes
// =============================================================================

@JsonPropertyOrder(Array("minReaderVersion", "minWriterVersion", "readerFeatures", "writerFeatures"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class ProtocolInfo(
    minReaderVersion: Int,
    minWriterVersion: Int,
    readerFeatures: Option[Seq[String]] = None,
    writerFeatures: Option[Seq[String]] = None)

@JsonPropertyOrder(Array("numAddFiles", "numRemoveFiles", "sizeInBytes", "numCommits",
  "numActions", "lastCheckpointVersion", "lastCrcVersion", "numCheckpointFiles"))
case class LogInfo(
    numAddFiles: Long,
    numRemoveFiles: Long,
    sizeInBytes: Long,
    numCommits: Int,
    numActions: Long,
    lastCheckpointVersion: Long,
    lastCrcVersion: Long,
    numCheckpointFiles: Int)

@JsonPropertyOrder(Array("numClusteringColumns", "numPartitionColumns", "numDistinctPartitions"))
case class DataLayoutInfo(
    numClusteringColumns: Int,
    numPartitionColumns: Int,
    numDistinctPartitions: Long)

@JsonPropertyOrder(Array("name", "description", "schema", "protocol", "logInfo",
  "properties", "dataLayout", "tags"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class TableInfo(
    name: String,
    description: String,
    schema: Any,
    protocol: ProtocolInfo,
    logInfo: LogInfo,
    properties: Map[String, String],
    dataLayout: DataLayoutInfo,
    tags: Option[Seq[String]] = None)

// =============================================================================
// Low-level action types
// =============================================================================

/**
 * Add-file action serialized in a low-level `commit`. `dataFile` points to a Parquet of LOGICAL
 * rows under `data/commit_N/`; on replay the engine writes it into the table (column-mapping- and
 * partition-aware, stats computed), so physical names/stats are derived per table, never stored.
 */
@JsonPropertyOrder(Array("dataFile", "dataChange"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class AddFileAction(
    dataFile: String,
    dataChange: Option[Boolean] = None)

/**
 * Remove-file action (tombstone) for low-level commits. References a prior low-level add by the
 * commit ordinal that produced it (`addedAtCommit`); the engine assigns file paths per table, so
 * the tombstone is resolved to the actual path(s) at replay. The tombstone inherits the live
 * add's partitionValues/size/stats (extendedFileMetadata).
 */
@JsonPropertyOrder(Array("addedAtCommit", "dataChange"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class RemoveFileAction(
    addedAtCommit: Int,
    dataChange: Option[Boolean] = None)

/** Logical `rows` for a low-level add (full schema incl. partition columns); engine-materialized, not serialized. */
case class AddFileInput(
    rows: Seq[Map[String, Any]],
    dataChange: Option[Boolean] = None)

/** Application transaction for idempotent low-level commits. */
@JsonPropertyOrder(Array("appId", "version"))
case class AppTxn(appId: String, version: Long)

/** Domain metadata entry for low-level commits (added domains). */
@JsonPropertyOrder(Array("domain", "configuration"))
case class AddDomainMetadata(
    domain: String,
    configuration: String)

// =============================================================================
// Write spec case classes
// =============================================================================

/**
 * A single commit in a write spec. Each operation is its own case class carrying only the
 * fields it uses, so illegal field combinations are unrepresentable and `match`es over the
 * hierarchy are exhaustive. The `operation` discriminator is written by Jackson as the leading
 * JSON property (`@JsonTypeInfo`); the in-memory `operation` accessor is `@JsonIgnore`d so it is
 * not also serialized as a duplicate field.
 *
 * High-level ops (create_table/replace_table/insert/update/delete/evolve_schema/
 * update_properties) replay from their parameters; the low-level [[LowLevelCommitOp]] replays its
 * recorded raw actions.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY,
  property = "operation")
@JsonSubTypes(Array(
  new JsonSubTypes.Type(value = classOf[CreateTableCommit], name = "create_table"),
  new JsonSubTypes.Type(value = classOf[ReplaceTableCommit], name = "replace_table"),
  new JsonSubTypes.Type(value = classOf[InsertCommit], name = "insert"),
  new JsonSubTypes.Type(value = classOf[DeleteCommit], name = "delete"),
  new JsonSubTypes.Type(value = classOf[UpdateCommit], name = "update"),
  new JsonSubTypes.Type(value = classOf[EvolveSchemaCommit], name = "evolve_schema"),
  new JsonSubTypes.Type(value = classOf[UpdatePropertiesCommit], name = "update_properties"),
  new JsonSubTypes.Type(value = classOf[LowLevelCommitOp], name = "commit")))
sealed trait WriteCommit {
  @JsonIgnore def operation: String
}

// ---- High-level operations (replayed from parameters) ----

@JsonPropertyOrder(Array("schema", "partitionColumns", "properties"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class CreateTableCommit(
    schema: Any,
    partitionColumns: Option[Seq[String]] = None,
    properties: Option[Map[String, String]] = None) extends WriteCommit {
  @JsonIgnore val operation = "create_table"
}

/**
 * `CREATE OR REPLACE TABLE` — replaces schema/partitioning/properties and all data. With `rows`
 * it is a replace-as-select (RTAS): a single commit that replaces the table and writes the rows
 * (replayed via `CREATE OR REPLACE TABLE … USING delta … AS SELECT … FROM VALUES`).
 */
@JsonPropertyOrder(Array("schema", "partitionColumns", "properties", "dataFiles"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class ReplaceTableCommit(
    schema: Any,
    partitionColumns: Option[Seq[String]] = None,
    properties: Option[Map[String, String]] = None,
    dataFiles: Option[Seq[String]] = None) extends WriteCommit {
  @JsonIgnore val operation = "replace_table"
}

@JsonPropertyOrder(Array("dataFiles"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class InsertCommit(dataFiles: Option[Seq[String]] = None) extends WriteCommit {
  @JsonIgnore val operation = "insert"
}

@JsonPropertyOrder(Array("predicate"))
case class DeleteCommit(predicate: String) extends WriteCommit {
  @JsonIgnore val operation = "delete"
}

@JsonPropertyOrder(Array("predicate", "set"))
case class UpdateCommit(predicate: String, set: Map[String, String]) extends WriteCommit {
  @JsonIgnore val operation = "update"
}

@JsonPropertyOrder(Array("addColumns", "renameColumns", "dropColumns"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class EvolveSchemaCommit(
    addColumns: Option[Any] = None,
    renameColumns: Option[Map[String, String]] = None,
    dropColumns: Option[Seq[String]] = None) extends WriteCommit {
  @JsonIgnore val operation = "evolve_schema"
}

@JsonPropertyOrder(Array("set", "remove"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class UpdatePropertiesCommit(
    set: Option[Map[String, String]] = None,
    remove: Option[Seq[String]] = None) extends WriteCommit {
  @JsonIgnore val operation = "update_properties"
}

// ---- Low-level operation (raw Delta actions; data written via the engine on replay) ----

@JsonPropertyOrder(Array("schema", "tableProperties", "txn", "addFiles", "removeFiles",
  "addDomainMetadata", "removeDomainMetadata"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class LowLevelCommitOp(
    schema: Option[Any] = None,
    tableProperties: Option[Map[String, String]] = None,
    txn: Option[AppTxn] = None,
    addFiles: Option[Seq[AddFileAction]] = None,
    removeFiles: Option[Seq[RemoveFileAction]] = None,
    addDomainMetadata: Option[Seq[AddDomainMetadata]] = None,
    removeDomainMetadata: Option[Seq[String]] = None) extends WriteCommit {
  @JsonIgnore val operation = "commit"
}

@JsonPropertyOrder(Array("type", "commits"))
case class WriteSpec(commits: Seq[WriteCommit]) {
  val `type`: String = "write"
}

/**
 * Layout conventions for a workload output directory, shared by capture and replay so the
 * "commit index -> data directory" decision lives in one place (not duplicated across
 * [[WriteSpecBuilder]] and [[WorkloadValidator]]).
 */
private[workload] object SpecLayout {
  /** Relative path (from the output dir) of the data directory for commit `idx`. */
  def commitDataDir(idx: Int): String = s"data/commit_$idx"

  /** Relative path (from the output dir) of `name` under commit `idx`'s data directory. */
  def commitDataFile(idx: Int, name: String): String = s"${commitDataDir(idx)}/$name"

  /**
   * The in-table `AddFile.path`s a commit produced, read from its `_delta_log/<version>.json`.
   * Because commit index == table version, a low-level remove's `addedAtCommit` ordinal is the
   * version whose adds it tombstones — resolved here against the actual (engine-assigned) paths.
   */
  def addPathsAt(tablePath: Path, version: Int): Seq[String] =
    if (!Files.exists(CommitLog.commitFile(tablePath, version))) Seq.empty
    else CommitLog.read(tablePath, version).collect { case a: AddFile => a.path }
}

// =============================================================================
// Delta log action case classes (for parsing commit JSON)
// =============================================================================

case class TxnAction(appId: String, version: Long)

case class TxnActionWrapper(txn: TxnAction)

// =============================================================================
// Last checkpoint case classes
// =============================================================================

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class V2CheckpointInfo(
    path: Option[String] = None,
    sizeInBytes: Option[Long] = None,
    nonFileActions: Option[Any] = None,
    sidecarFiles: Option[Any] = None)

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class LastCheckpointInfo(
    version: Long,
    size: Option[Long] = None,
    sizeInBytes: Option[Long] = None,
    numOfAddFiles: Option[Long] = None,
    checkpointSchema: Option[Any] = None,
    checksum: Option[String] = None,
    v2Checkpoint: Option[V2CheckpointInfo] = None)

// =============================================================================
// JSON and DataFrame utilities
// =============================================================================

object JsonUtil {

  val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.enable(DeserializationFeature.USE_LONG_FOR_INTS)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  private val prettyWriter = mapper.writerWithDefaultPrettyPrinter()

  def writeSpec(path: Path, spec: Any): Unit =
    Files.write(path, prettyWriter.writeValueAsBytes(spec))

  // Arrays whose element order is NOT semantically meaningful (they are sets) and so are
  // sorted before comparison. Everything else (partitionColumns, schema `fields`, ...) keeps
  // its order.
  private val OrderInsensitiveArrays = Set("readerFeatures", "writerFeatures")

  /**
   * Canonicalize protocol/metadata JSON for engine-neutral comparison: recursively sort
   * object keys, sort the order-insensitive feature arrays, preserve order-significant arrays,
   * and parse + canonicalize the `schemaString` (a JSON document embedded as a string).
   */
  def canonicalJson(value: Any): String =
    mapper.writeValueAsString(canonicalizeNode(mapper.valueToTree[JsonNode](value), ""))

  private def canonicalizeNode(node: JsonNode, key: String): JsonNode = node match {
    case obj: ObjectNode =>
      val out = mapper.createObjectNode()
      obj.fieldNames().asScala.toSeq.sorted.foreach(k => out.set[JsonNode](k, canonicalizeNode(obj.get(k), k)))
      out
    case arr: ArrayNode =>
      val children = arr.elements().asScala.map(canonicalizeNode(_, key)).toSeq
      val ordered = if (OrderInsensitiveArrays.contains(key)) children.sortBy(_.toString) else children
      val out = mapper.createArrayNode()
      ordered.foreach(out.add)
      out
    case t: TextNode if key == "schemaString" =>
      // schemaString is a JSON document stored as a string; canonicalize its contents too.
      TextNode.valueOf(mapper.writeValueAsString(canonicalizeNode(mapper.readTree(t.asText()), "")))
    case other => other
  }

  def readReadSpec(path: Path): ReadSpec =
    mapper.readValue(Files.readAllBytes(path), classOf[ReadSpec])

  def readSnapshotSpec(path: Path): SnapshotSpec =
    mapper.readValue(Files.readAllBytes(path), classOf[SnapshotSpec])

  def readWriteSpec(path: Path): WriteSpec =
    mapper.readValue(Files.readAllBytes(path), classOf[WriteSpec])

  def toRowMultiset(df: DataFrame): Map[String, Int] = {
    // Drop MAP-type columns that Spark can't serialize to JSON
    val nonMapCols = df.schema.fields
      .filter(f => !f.dataType.isInstanceOf[org.apache.spark.sql.types.MapType])
      .map(f => df.col(s"`${f.name.replace("`", "``")}`"))
    val filteredDf = if (nonMapCols.length < df.schema.fields.length) {
      df.select(nonMapCols: _*)
    } else df
    filteredDf.toJSON.collect().groupBy(identity).map { case (k, v) => k -> v.length }
  }

  def columnRef(name: String): Column =
    if (name.startsWith("_metadata.")) col(name)
    else col(s"`${name.replace("`", "``")}`")

  def buildDeltaReader(spark: SparkSession, tablePath: Path,
      version: Option[Long], timestamp: Option[String]): DataFrame = {
    var reader = spark.read.format("delta")
    version.foreach(v => reader = reader.option("versionAsOf", v))
    timestamp.foreach(ts => reader = reader.option("timestampAsOf", ts))
    reader.load(tablePath.toString)
  }

  def applyFilters(df: DataFrame, predicate: Option[String],
      columns: Option[Seq[String]]): DataFrame = {
    var result = df
    predicate.foreach(p => result = result.filter(p))
    columns.foreach(cols => result = result.select(cols.map(columnRef): _*))
    result
  }

  def extractErrorCode(e: Throwable): String = e match {
    case st: org.apache.spark.SparkThrowable =>
      Option(st.getErrorClass).getOrElse(e.getClass.getSimpleName)
    case _ => e.getClass.getSimpleName
  }

  def normalizeErrorCode(code: String): String = code match {
    case "DeltaIllegalStateException" => "DELTA_STATE_RECOVER_ERROR"
    // Version not found errors
    case "IllegalStateException" | "DELTA_LOG_FILE_NOT_FOUND" => "DELTA_VERSION_NOT_FOUND"
    // Timestamp invalid errors
    case "IllegalArgumentException" => "DELTA_TIMESTAMP_INVALID"
    case other => other
  }

  def resolveSnapshot(spark: SparkSession, log: LogView, tablePath: String,
      version: Option[Long],
      timestamp: Option[String]): SnapshotView = {
    (version, timestamp) match {
      case (Some(v), _) => log.getSnapshotAt(v)
      case (_, Some(ts)) =>
        val tsValue = java.sql.Timestamp.valueOf(ts)
        val histDf = spark.sql(s"DESCRIBE HISTORY delta.`$tablePath`")
        val rows = histDf
          .filter(col("timestamp") <= lit(tsValue))
          .orderBy(col("version").desc)
          .select("version")
          .take(1)
        if (rows.isEmpty) {
          // Timestamp is before any commit — let the Delta reader produce the
          // proper error (e.g. DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION).
          spark.read.format("delta").option("timestampAsOf", ts).load(tablePath).count()
          // If somehow no error, fall back to latest snapshot
          log.update()
        } else {
          log.getSnapshotAt(rows(0).getLong(0))
        }
      case _ => log.update()
    }
  }

  def assertMultisetsEqual(expected: Map[String, Int], actual: Map[String, Int],
      specName: String): Unit = {
    if (expected != actual) {
      val missing = expected.keySet -- actual.keySet
      val extra = actual.keySet -- expected.keySet
      val countMismatches = (expected.keySet & actual.keySet).filter(k => expected(k) != actual(k))
      val details = new StringBuilder()
      if (missing.nonEmpty) {
        details.append(s"\n  Missing rows: ${missing.size}")
        missing.take(3).foreach(r => details.append(s"\n    $r"))
      }
      if (extra.nonEmpty) {
        details.append(s"\n  Extra rows: ${extra.size}")
        extra.take(3).foreach(r => details.append(s"\n    $r"))
      }
      if (countMismatches.nonEmpty) {
        details.append(s"\n  Count mismatches: ${countMismatches.size}")
        countMismatches.take(3).foreach { r =>
          details.append(s"\n    expected ${expected(r)}x, got ${actual(r)}x: $r")
        }
      }
      throw new RuntimeException(
        s"Validation FAILED for $specName: row-level mismatch" +
          s" (expected ${expected.values.sum}, got ${actual.values.sum})$details")
    }
  }
}
