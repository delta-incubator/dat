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

package io.delta.workload.deltaharness

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Platform-specific backing for Delta-internal access.
 *
 * Implementations wrap either OSS delta-spark (`org.apache.spark.sql.delta.DeltaLog`)
 * or DBR tahoe (`com.databricks.sql.transaction.tahoe.DeltaLog`). The generator
 * library depends only on this SPI.
 *
 * Discovery order:
 *   1. `-Dio.delta.workload.harness=<fully-qualified-class-name>` (explicit override)
 *   2. If `com.databricks.sql.transaction.tahoe.DeltaLog` is on the classpath,
 *      `io.delta.workload.deltaharness.dbr.DbrDeltaHarness` is used.
 *   3. Otherwise `io.delta.workload.deltaharness.oss.OssDeltaHarness` is used.
 */
trait DeltaHarness {
  /**
   * Open a log view at `tablePath`. Implementations MUST clear any internal
   * DeltaLog cache before returning — consumers always see a fresh view.
   * This keeps test isolation guarantees simple at the SPI level and makes
   * cache management an adapter-internal concern.
   */
  def openLog(spark: SparkSession, tablePath: String): LogView
}

trait LogView {
  def update(): SnapshotView
  def getSnapshotAt(version: Long): SnapshotView
  def checkpoint(): Unit
}

trait SnapshotView {
  def version: Long
  /** Raw JSON of the Protocol action, e.g. `{"protocol":{"minReaderVersion":...}}`. */
  def protocolJson: String
  /** Raw JSON of the Metadata action, e.g. `{"metaData":{"id":...}}`. */
  def metadataJson: String
  /**
   * All active add-file actions. Canonical columns:
   *  - `path` (String)
   *  - `size` (Long)
   *  - `partitionValues` (Map<String, String>)
   *  - `json` (String — raw AddFile action JSON, i.e. `{"add":{...}}`)
   *
   * Adapters may include additional columns; consumers must not depend on
   * ordering or extra fields.
   */
  def allFiles: DataFrame
}

object DeltaHarness {
  private val OssClass = "io.delta.workload.deltaharness.oss.OssDeltaHarness"
  private val DbrClass = "io.delta.workload.deltaharness.dbr.DbrDeltaHarness"

  private lazy val instance: DeltaHarness = {
    val className = sys.props.get("io.delta.workload.harness").getOrElse {
      try {
        Class.forName("com.databricks.sql.transaction.tahoe.DeltaLog")
        DbrClass
      } catch {
        case _: ClassNotFoundException => OssClass
      }
    }
    Class.forName(className).getDeclaredConstructor()
      .newInstance().asInstanceOf[DeltaHarness]
  }

  def get: DeltaHarness = instance
}
