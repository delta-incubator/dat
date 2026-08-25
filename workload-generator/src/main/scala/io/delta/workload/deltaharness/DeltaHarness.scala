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

import java.nio.file.Path

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

import io.delta.workload.model.{AddDomainMetadata, AppTxn}

/**
 * A remove-file action (tombstone) for a low-level commit, by in-table `path`. The implementation
 * tombstones the matching active file via the engine, so the tombstone inherits its
 * `partitionValues`/`size`/`stats` (column-mapping- and partition-correct) — the caller supplies
 * only the path and the `dataChange` flag.
 */
case class CommitRemoveFile(path: String, dataChange: Boolean)

/**
 * A platform-neutral description of a low-level commit. Carries only plain Scala types and the
 * generator's neutral action case classes, never `org.apache.spark.sql.delta.*`.
 *
 * @param addDataParquet       Parquet files of LOGICAL rows to add; the engine reads each and writes
 *                             it into the table (column-mapping aware, computing stats/layout)
 * @param schemaJson           `StructType.json` to set as `metadata.schemaString`, or None to keep
 * @param properties           properties merged into `metadata.configuration`, or None to keep
 * @param setTransaction       optional `SetTransaction` (appId/version) action
 * @param removeFiles          remove-file actions to tombstone (by in-table path)
 * @param addDomainMetadata    domain-metadata entries to add
 * @param removeDomainMetadata domain names to tombstone
 */
case class CommitRequest(
    addDataParquet: Seq[String] = Nil,
    schemaJson: Option[String] = None,
    properties: Option[Map[String, String]] = None,
    setTransaction: Option[AppTxn] = None,
    removeFiles: Seq[CommitRemoveFile] = Nil,
    addDomainMetadata: Seq[AddDomainMetadata] = Nil,
    removeDomainMetadata: Seq[String] = Nil)

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

  /**
   * Open a transaction at `tablePath`, apply the metadata update (schema/properties) if present,
   * write any `req.addDataParquet` data files through the engine's own write path (honoring column
   * mapping and partitioning, computing stats), and commit the produced AddFile actions together
   * with `req`'s other actions in one `DeltaOperations.ManualUpdate`.
   *
   * Implementations MUST clear any internal DeltaLog cache before and after committing, mirroring
   * the `openLog` cache-clearing contract so consumers always see a fresh view.
   *
   * @return the in-table `AddFile.path`s the engine produced (in `addDataParquet` order; a file
   *         may yield several adds when partitioned), so callers can reference them later.
   */
  def commit(spark: SparkSession, tablePath: String, req: CommitRequest): Seq[String]

  /** The table's schema at `version` (latest if None). */
  def schemaAt(spark: SparkSession, tablePath: String, version: Option[Long]): StructType

  /**
   * Materialize in-memory `rows` (the workload-generator's authoring surface) into a single Parquet
   * file at `dest`, coercing each value to the corresponding column type in `schema`. Capture writes
   * rows here at generation time — no Delta-log scan — and replay reads the produced files back.
   */
  def writeRows(
      spark: SparkSession, schema: StructType, rows: Seq[Map[String, Any]], dest: Path): Unit
}

trait LogView {
  def update(): ResolvedSnapshot
  def getSnapshotAt(version: Long): ResolvedSnapshot
  def checkpoint(): Unit
}

trait ResolvedSnapshot {
  def version: Long
  /** Typed protocol + metadata for this snapshot. */
  def snapshot: Snapshot
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

// Neutral typed view of a snapshot's protocol + metadata. SPI-side types (no dependency on
// io.delta.workload): `StructType` is Spark, the rest are plain Scala, so the engine boundary
// hands back types, not JSON for callers to re-parse.
case class Protocol(
    minReaderVersion: Int,
    minWriterVersion: Int,
    readerFeatures: Option[Seq[String]],
    writerFeatures: Option[Seq[String]])

case class Format(provider: String, options: Map[String, String])

case class Metadata(
    id: String,
    name: Option[String],
    description: Option[String],
    format: Format,
    schema: StructType,
    partitionColumns: Seq[String],
    createdTime: Option[Long],
    configuration: Map[String, String])

case class Snapshot(version: Long, protocol: Protocol, metadata: Metadata)

object DeltaHarness {
  private val DefaultClass = "io.delta.workload.deltaharness.DeltaSparkHarness"

  private lazy val instance: DeltaHarness = {
    val className = sys.props.getOrElse("io.delta.workload.harness", DefaultClass)
    Class.forName(className).getDeclaredConstructor()
      .newInstance().asInstanceOf[DeltaHarness]
  }

  def get: DeltaHarness = instance
}
