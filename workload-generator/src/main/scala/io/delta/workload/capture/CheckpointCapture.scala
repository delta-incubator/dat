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

package io.delta.workload.capture

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession

import io.delta.workload.deltaharness.{DeltaHarness, ResolvedSnapshot}
import io.delta.workload.json.JsonUtil
import io.delta.workload.model.{CheckpointExpected, CheckpointFile, CheckpointSpec, MetadataInfo, ProtocolInfo}

/**
 * Captures and validates checkpoint specs. A checkpoint is forced at the target version via
 * the neutral `LogView.checkpoint` SPI, then its reconstructed state (protocol, metadata,
 * set-transactions, domain-metadata) is read back through the neutral `ResolvedSnapshot`
 * accessors and recorded inline in the spec JSON, mirroring [[SnapshotCapture]].
 *
 * Scope: targets classic V1 single-file checkpoints. The `.checkpoint.parquet` produced by
 * `checkpoint(version)` is left in place under the table's `_delta_log/` so consumers can exercise
 * checkpoint reads.
 */
object CheckpointCapture {

  def capture(
      spark: SparkSession, testId: String, tablePath: Path, specsDir: Path,
      version: Long, name: String): Path = {

    val specName = s"${testId}_$name"
    val specPath = specsDir.resolve(s"$specName.json")

    val harness = DeltaHarness.get
    harness.openLog(spark, tablePath.toString).checkpoint(version)
    require(checkpointFileExists(tablePath, version),
      s"Checkpoint $specName: no checkpoint file present at version $version after checkpoint()")

    val snapshot = harness.openLog(spark, tablePath.toString).getSnapshotAt(version)
    val expected = Some(buildExpected(snapshot))

    val spec = CheckpointSpec(version, expected)
    JsonUtil.writeSpec(specPath, spec)
    validateFromSpec(spark, tablePath, specPath)

    println(s"  Checkpoint captured: $specName (version=$version)")
    specPath
  }

  /**
   * Validate a checkpoint spec Protocol/metadata/txn/domain-metadata against the table at `tablePath`.
   */
  def validateFromSpec(
      spark: SparkSession, tablePath: Path, specPath: Path,
      isWriteValidation: Boolean = false): Unit = {
    val spec = JsonUtil.readSpecAs(specPath, classOf[CheckpointSpec])
    val specName = specPath.getFileName.toString.stripSuffix(".json")
    val harness = DeltaHarness.get

    if (isWriteValidation) {
      harness.openLog(spark, tablePath.toString).checkpoint(spec.version)
      require(checkpointFileExists(tablePath, spec.version),
        s"Checkpoint validation failed for $specName: no checkpoint file at version " +
          s"${spec.version} after checkpoint()")
    }

    spec.expected.foreach { exp =>
      val resolved = harness.openLog(spark, tablePath.toString).getSnapshotAt(spec.version)
      val live = resolved.snapshot
      SnapshotCapture.assertMatches(exp.protocol, live.protocol, exp.metadata, live.metadata,
        isWriteValidation, specName)
      require(exp.txn.getOrElse(Seq.empty).toSet == resolved.setTransactions.toSet,
        s"Checkpoint validation failed for $specName: txn mismatch")
      require(exp.domainMetadata.getOrElse(Seq.empty).toSet == resolved.domainMetadata.toSet,
        s"Checkpoint validation failed for $specName: domainMetadata mismatch")
      if (!isWriteValidation) {
        require(exp.files.toSet == fileSet(resolved).toSet,
          s"Checkpoint validation failed for $specName: file set mismatch")
      }
    }
  }

  /** Build the expected checkpoint state from a snapshot via the neutral SPI accessors. */
  private def buildExpected(snapshot: ResolvedSnapshot): CheckpointExpected = {
    val txn = snapshot.setTransactions
    val domainMetadata = snapshot.domainMetadata
    CheckpointExpected(
      protocol = ProtocolInfo.from(snapshot.snapshot.protocol),
      metadata = MetadataInfo.from(snapshot.snapshot.metadata),
      files = fileSet(snapshot),
      txn = if (txn.nonEmpty) Some(txn) else None,
      domainMetadata = if (domainMetadata.nonEmpty) Some(domainMetadata) else None)
  }

  private def fileSet(s: ResolvedSnapshot): Seq[CheckpointFile] =
    s.allFiles.select("path", "size").collect()
      .map(r => CheckpointFile(r.getString(0), r.getLong(1))).toSeq

  /** True iff a classic checkpoint file (`<version>.checkpoint*`) exists in `_delta_log`. */
  private def checkpointFileExists(tablePath: Path, version: Long): Boolean = {
    val logDir = tablePath.resolve("_delta_log")
    val prefix = f"$version%020d.checkpoint"
    val stream = Files.list(logDir)
    try stream.iterator().asScala.exists(_.getFileName.toString.startsWith(prefix))
    finally stream.close()
  }
}
