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

package io.delta.workload.validate

import java.nio.file.Path

import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import io.delta.workload.deltaharness.DeltaHarness
import io.delta.workload.json.JsonUtil

/**
 * Validates the per-file statistics an engine wrote, on a replayed (engine-written) table.
 *
 * This is a PROPERTY check, deliberately not an expected-value compare against the corpus: the
 * Delta protocol mandates stats SOUNDNESS and FORMAT, but leaves tightness (and even presence)
 * optional, so a sound writer may legitimately emit wider — or no — stats. Comparing to a captured
 * expected would false-fail such a writer; asserting the invariants does not.
 *
 * TODO: also check `numRecords` soundness (== the data file's row count) and per-column min/max
 * soundness (stats.min <= real_min <= real_max <= stats.max); both need reading the (possibly
 * URL-encoded) data-file paths.
 */
object StatsValidator {

  /** Validate the stats on the table at `tablePath`. Returns findings; empty means compliant. */
  def validate(spark: SparkSession, tablePath: Path): Seq[String] = {
    val snapshot = DeltaHarness.get.openLog(spark, tablePath.toString).update()
    val timestampCols = timestampColumns(snapshot.snapshot.metadata.schema)
    if (timestampCols.isEmpty) return Seq.empty // no timestamp columns to check

    val findings = mutable.ArrayBuffer[String]()
    val addJsons = snapshot.allFiles.select("json").collect().map(_.getString(0))
    for (addJson <- addJsons) {
      val add = JsonUtil.mapper.readTree(addJson).path("add")
      val statsNode = add.path("stats")
      if (statsNode.isTextual) {
        val stats = JsonUtil.mapper.readTree(statsNode.asText)
        checkTimestampPrecision(add.path("path").asText(""), stats, timestampCols, findings)
      }
    }
    findings.toSeq
  }

  /**
   * Segment lists of every timestamp / timestamp_ntz field, recursing through nested structs
   * (a struct-nested timestamp is serialized in stats under the same nesting, e.g. `info` -> `ts`).
   * Returning segment lists rather than dotted strings avoids the ambiguity between a top-level
   * column literally named `a.b` and a nested `a` -> `b`.
   * Descends only into structs: Delta collects no min/max stats inside arrays or maps, and
   * partition columns live in `partitionValues` with a different serialization (out of scope).
   * (`typeName` avoids depending on the `TimestampNTZType` symbol, which older Spark versions lack.)
   */
  private def timestampColumns(schema: StructType): Set[Seq[String]] = {
    def collect(struct: StructType, prefix: Seq[String]): Seq[Seq[String]] =
      struct.fields.toSeq.flatMap { f =>
        val path = prefix :+ f.name
        f.dataType match {
          case nested: StructType => collect(nested, path)
          case dt if dt.typeName == "timestamp" || dt.typeName == "timestamp_ntz" => Seq(path)
          case _ => Seq.empty
        }
      }
    collect(schema, Nil).toSet
  }

  /**
   * Timestamp-precision check: a timestamp min/max stat must be millisecond-truncated (<= 3
   * fractional digits). Delta serializes stats timestamps as ISO-8601 at millisecond granularity;
   * microsecond (`.298677Z`) or nanosecond values violate the protocol and can break skipping
   * under some readers.
   */
  private def checkTimestampPrecision(
      path: String,
      stats: com.fasterxml.jackson.databind.JsonNode,
      timestampPaths: Set[Seq[String]],
      findings: mutable.ArrayBuffer[String]): Unit = {
    for (bound <- Seq("minValues", "maxValues")) {
      val boundNode = stats.path(bound)
      for (colPath <- timestampPaths) {
        // Walk the path segments into the (possibly nested) stats object.
        val v = colPath.foldLeft(boundNode)((node, seg) => node.path(seg))
        if (v.isTextual) {
          fractionalDigits(v.asText).foreach { n =>
            if (n > 3) {
              val col = colPath.mkString(".")
              findings += s"$path: $bound.$col = '${v.asText}' has $n fractional-second digits; " +
                "Delta stats timestamps must be millisecond-truncated (<= 3)"
            }
          }
        }
      }
    }
  }

  /** Digit count of the fractional-seconds part of an ISO-8601-ish timestamp, or None if absent. */
  private def fractionalDigits(s: String): Option[Int] = {
    val dot = s.indexOf('.')
    if (dot < 0) return None
    val n = s.drop(dot + 1).takeWhile(_.isDigit).length
    if (n > 0) Some(n) else None
  }
}
