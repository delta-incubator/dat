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

import org.apache.spark.sql.SparkSession

import io.delta.workload.deltaharness.DeltaHarness
import io.delta.workload.json.JsonUtil
import io.delta.workload.model.{CrcExpected, CrcSpec, ProtocolInfo}

/**
 * Captures and validates CRC (version-checksum) specs. The `<version>.crc` file is plain JSON
 * in `_delta_log/`, so it is read directly with [[java.nio.file.Files]] + [[JsonUtil.mapper]] —
 * no Delta class and no SPI method are needed. The protocol is read from the neutral
 * `ResolvedSnapshot` so it compares engine-neutrally; all other fields come straight from the
 * `.crc`, keyed by delta-spark's `VersionChecksum` field names. Optional/feature-gated fields
 * (deletion-vector counts, set transactions, inCommitTimestamp) are recorded only when the `.crc`
 * carries them, and validated conditionally.
 */
object CrcCapture {

  /**
   * Capture a crc spec for `version`, or skip if no `.crc` exists there.
   *
   * A crc spec asserts the *contents* of a `<version>.crc` where one is present; absence is a
   * skip, not a failure. Some engines flush `.crc` asynchronously, so the latest
   * commit's `.crc` is often not on disk at read time. When the target `.crc` is missing this
   * logs a SKIP, emits no spec, and returns `None`.
   *
   * @return `Some(specPath)` if a spec was emitted and self-validated, `None` if skipped (no `.crc`).
   */
  def capture(
      spark: SparkSession, testId: String, tablePath: Path, specsDir: Path,
      version: Long, name: String): Option[Path] = {

    val specName = s"${testId}_$name"
    val specPath = specsDir.resolve(s"$specName.json")

    val crcNode = readCrc(tablePath, version) match {
      case Some(node) => node
      case None =>
        println(s"  CRC skipped: $specName (version=$version): " +
          s"no .crc at version $version; engine did not write/flush one")
        return None
    }

    val harness = DeltaHarness.get
    val snapshot = harness.openLog(spark, tablePath.toString).getSnapshotAt(version)
    val protocol = ProtocolInfo.from(snapshot.snapshot.protocol)

    val expected = CrcExpected(
      tableSizeBytes = JsonUtil.crcLongField(crcNode, "tableSizeBytes"),
      numFiles = JsonUtil.crcLongField(crcNode, "numFiles"),
      numDeletionVectors = JsonUtil.crcLongField(crcNode, "numDeletionVectorsOpt"),
      numDeletedRecords = JsonUtil.crcLongField(crcNode, "numDeletedRecordsOpt"),
      inCommitTimestamp = JsonUtil.crcLongField(crcNode, "inCommitTimestampOpt"),
      protocol = Some(protocol),
      setTransactions = JsonUtil.crcSetTransactions(crcNode))

    val spec = CrcSpec(version, Some(expected))
    JsonUtil.writeSpec(specPath, spec)
    validateFromSpec(spark, tablePath, specPath)

    println(s"  CRC captured: $specName (version=$version)")
    Some(specPath)
  }

  /**
   * Validate a crc spec's (deletion-vector / deleted-record counts), protocol, and set transactions
   * against the table at `tablePath`.
   */
  def validateFromSpec(
      spark: SparkSession, tablePath: Path, specPath: Path,
      isWriteValidation: Boolean = false): Unit = {
    val spec = JsonUtil.readSpecAs(specPath, classOf[CrcSpec])
    val specName = specPath.getFileName.toString.stripSuffix(".json")
    val harness = DeltaHarness.get

    spec.expected.foreach { exp =>
      val crcNode = readCrc(tablePath, spec.version) match {
        case Some(node) => node
        case None =>
          // Validate-where-present: a missing `.crc` (e.g. async flush) is a skip, not a failure.
          println(s"  CRC validation skipped for $specName: " +
            s"no .crc at version ${spec.version}; engine did not write/flush one")
          return
      }

      // === Aggregate fields (VersionChecksum keys) ===
      val scalarFields =
        if (isWriteValidation) ScalarFields.filterNot { case (l, _, _) => NonReproducibleScalarFields(l) }
        else ScalarFields
      scalarFields.foreach { case (label, key, get) =>
        requireLong(specName, label, get(exp), JsonUtil.crcLongField(crcNode, key))
      }

      // === Protocol (engine-neutral, via ResolvedSnapshot) ===
      exp.protocol.foreach { expProtocol =>
        val snapshot = harness.openLog(spark, tablePath.toString).getSnapshotAt(spec.version)
        require(expProtocol == ProtocolInfo.from(snapshot.snapshot.protocol),
          s"CRC validation failed for $specName: protocol mismatch")
      }

      // === Set transactions ===
      exp.setTransactions.foreach { expTxn =>
        require(Some(expTxn.toSet) == JsonUtil.crcSetTransactions(crcNode).map(_.toSet),
          s"CRC validation failed for $specName: setTransactions mismatch")
      }
    }
  }

  // Scalar `VersionChecksum` fields checked uniformly: (spec label, `.crc` key, spec accessor).
  private val ScalarFields: Seq[(String, String, CrcExpected => Option[Long])] = Seq(
    ("tableSizeBytes",     "tableSizeBytes",       _.tableSizeBytes),
    ("numFiles",           "numFiles",             _.numFiles),
    ("numDeletionVectors", "numDeletionVectorsOpt", _.numDeletionVectors),
    ("numDeletedRecords",  "numDeletedRecordsOpt",  _.numDeletedRecords),
    ("inCommitTimestamp",  "inCommitTimestampOpt",  _.inCommitTimestamp))

  private val NonReproducibleScalarFields: Set[String] =
    Set("tableSizeBytes", "numFiles", "inCommitTimestamp")

  /** Read the `<version>.crc` JSON, or `None` when no such file is on disk. */
  private def readCrc(
      tablePath: Path, version: Long): Option[com.fasterxml.jackson.databind.JsonNode] = {
    val crcFile = tablePath.resolve("_delta_log").resolve(f"$version%020d.crc")
    if (!Files.exists(crcFile)) None
    else Some(JsonUtil.mapper.readTree(new String(Files.readAllBytes(crcFile), "UTF-8")))
  }

  // A field the spec recorded (Some) must be present in the `.crc` under the same key: a
  // spec-Some / crc-None mismatch means the writer dropped a field it should have written.
  // A field the spec did not record (None, e.g. a feature that was off) is nothing to check.
  private def requireLong(specName: String, field: String,
      expected: Option[Long], actual: Option[Long]): Unit =
    (expected, actual) match {
      case (Some(e), Some(a)) =>
        require(e == a, s"CRC validation failed for $specName: $field expected=$e actual=$a")
      case (Some(e), None) =>
        require(false, s"CRC validation failed for $specName: $field expected=$e " +
          "but the .crc has no such field")
      case (None, _) => // spec did not record this field
    }
}
