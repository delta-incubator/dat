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

package io.delta.workload.engine

import java.nio.file.Path

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import io.delta.workload.deltaharness.{LogView, ResolvedSnapshot}

// =============================================================================
// Snapshot resolution / Delta reader
//
// The "read the Delta table / time-travel" concern: resolve a snapshot at a version or timestamp,
// build a time-travel Delta reader, apply predicate/column filters, and format/parse the wall-clock
// timestamp strings the spec and Spark's timestampAsOf share.
// =============================================================================

object SnapshotResolver {

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

  /**
   * Build a Change Data Feed reader over a version/timestamp range.
   */
  def buildCdfReader(spark: SparkSession, tablePath: Path,
      startVersion: Option[Long], endVersion: Option[Long],
      startTimestamp: Option[String], endTimestamp: Option[String],
      latestVersion: Long): DataFrame = {
    var reader = spark.read.format("delta").option("readChangeFeed", "true")
    startVersion.foreach(v => reader = reader.option("startingVersion", v))
    startTimestamp.foreach(ts => reader = reader.option("startingTimestamp", ts))
    endVersion.foreach(v => reader = reader.option("endingVersion", v))
    endTimestamp.foreach(ts => reader = reader.option("endingTimestamp", ts))
    if (endVersion.isEmpty && endTimestamp.isEmpty) {
      reader = reader.option("endingVersion", latestVersion)
    }
    reader.load(tablePath.toString)
  }

  def applyFilters(df: DataFrame, predicate: Option[String],
      columns: Option[Seq[String]]): DataFrame = {
    var result = df
    predicate.foreach(p => result = result.filter(p))
    columns.foreach(cols => result = result.select(cols.map(columnRef): _*))
    result
  }

  /** The timestampAsOf-safe wall-clock pattern Spark and the on-disk spec use. */
  private val TimestampPattern = "yyyy-MM-dd HH:mm:ss.SSS"

  private def sessionZone(spark: SparkSession): java.time.ZoneId =
    java.time.ZoneId.of(spark.conf.get("spark.sql.session.timeZone", "UTC"))

  /**
   * Format an [[java.time.Instant]] to the `yyyy-MM-dd HH:mm:ss.SSS` wall-clock string (in the
   * session time zone) that Spark's `timestampAsOf` and the on-disk spec need. The pipeline carries
   * `Instant` end-to-end; this is the single formatting edge.
   */
  def formatTimestamp(spark: SparkSession, instant: java.time.Instant): String =
    java.time.format.DateTimeFormatter.ofPattern(TimestampPattern)
      .withZone(sessionZone(spark)).format(instant)

  /** Parse a `yyyy-MM-dd HH:mm:ss.SSS` wall-clock string (session TZ) back to an [[java.time.Instant]]. */
  def parseTimestamp(spark: SparkSession, text: String): java.time.Instant =
    java.time.LocalDateTime
      .parse(text, java.time.format.DateTimeFormatter.ofPattern(TimestampPattern))
      .atZone(sessionZone(spark)).toInstant

  def resolveSnapshot(spark: SparkSession, log: LogView, tablePath: String,
      version: Option[Long],
      timestamp: Option[String]): ResolvedSnapshot = {
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
          // Timestamp is before any commit: the read must raise a timestamp-out-of-range error.
          spark.read.format("delta").option("timestampAsOf", ts).load(tablePath).count()
          throw new IllegalStateException(
            s"timestamp $ts precedes the first commit but the read did not error")
        } else {
          log.getSnapshotAt(rows(0).getLong(0))
        }
      case _ => log.update()
    }
  }
}
