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

import com.fasterxml.jackson.annotation._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import io.delta.workload.deltaharness.{LogView, SnapshotView}

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

@JsonPropertyOrder(Array("type", "version", "timestamp", "predicate", "columns", "expected", "expectedError"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class ReadSpec(
    version: Option[Long] = None,
    timestamp: Option[String] = None,
    predicate: Option[String] = None,
    columns: Option[Seq[String]] = None,
    expected: Option[ReadExpected] = None,
    expectedError: Option[SpecError] = None) {
  val `type`: String = "read"
}

@JsonPropertyOrder(Array("type", "version", "timestamp", "expected", "expectedError"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class SnapshotSpec(
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

/** Deletion vector descriptor for low-level commits. */
@JsonPropertyOrder(Array("storageType", "pathOrInlineDv", "offset", "sizeInBytes", "cardinality"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class DeletionVectorAction(
    storageType: String,
    pathOrInlineDv: String,
    offset: Option[Int] = None,
    sizeInBytes: Int,
    cardinality: Long)

/** Add file action for low-level commits. */
@JsonPropertyOrder(Array("dataFile", "partitionValues", "dataChange", "deletionVector"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class AddFileAction(
    dataFile: String,
    partitionValues: Option[Map[String, String]] = None,
    dataChange: Option[Boolean] = None,
    deletionVector: Option[DeletionVectorAction] = None)

/** Remove file action for low-level commits. */
@JsonPropertyOrder(Array("path", "dataChange"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class RemoveFileAction(
    path: String,
    dataChange: Option[Boolean] = None)

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

  def readReadSpec(path: Path): ReadSpec =
    mapper.readValue(Files.readAllBytes(path), classOf[ReadSpec])

  def readSnapshotSpec(path: Path): SnapshotSpec =
    mapper.readValue(Files.readAllBytes(path), classOf[SnapshotSpec])

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
