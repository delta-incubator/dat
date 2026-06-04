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
 * The default implementation wraps delta-spark (`org.apache.spark.sql.delta.DeltaLog`).
 * The generator library depends only on this SPI; an alternate backing can be selected via
 * `-Dio.delta.workload.harness=<fully-qualified-class-name>`.
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
  private val DefaultClass = "io.delta.workload.deltaharness.DeltaSparkHarness"

  private lazy val instance: DeltaHarness = {
    val className = sys.props.getOrElse("io.delta.workload.harness", DefaultClass)
    Class.forName(className).getDeclaredConstructor()
      .newInstance().asInstanceOf[DeltaHarness]
  }

  def get: DeltaHarness = instance
}
