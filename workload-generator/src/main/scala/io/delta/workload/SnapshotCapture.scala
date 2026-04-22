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

import java.nio.file.Path

import org.apache.spark.sql.SparkSession

import io.delta.workload.deltaharness.DeltaHarness

object SnapshotCapture {

  def capture(
      spark: SparkSession, testId: String, tablePath: Path, specsDir: Path,
      version: Option[Long] = None, timestamp: Option[String] = None,
      expectError: Option[String] = None): Unit = {

    require(!(version.isDefined && timestamp.isDefined),
      "Cannot specify both version and timestamp")

    val specName = (version, timestamp) match {
      case (Some(v), _) => s"${testId}_snapshot_v$v"
      case (_, Some(ts)) => s"${testId}_snapshot_ts_${ts.replace(":", "-").replace(" ", "_")}"
      case _ => s"${testId}_snapshot"
    }
    val specPath = specsDir.resolve(s"$specName.json")

    val harness = DeltaHarness.get

    val (expected, expectedError, resolvedVersion) = try {
      val log = harness.openLog(spark, tablePath.toString)
      val snapshot = JsonUtil.resolveSnapshot(spark, log, tablePath.toString, version, timestamp)
      if (snapshot.version < 0) {
        (None, Some(SpecError("DELTA_TABLE_NOT_FOUND",
          s"No valid Delta table at ${tablePath}")), None)
      } else {
        val protocol = JsonUtil.mapper.treeToValue(
          JsonUtil.mapper.readTree(snapshot.protocolJson).get("protocol"), classOf[Any])
        val metadata = JsonUtil.mapper.treeToValue(
          JsonUtil.mapper.readTree(snapshot.metadataJson).get("metaData"), classOf[Any])
        (Some(SnapshotExpected(protocol, metadata)), None, Some(snapshot.version))
      }
    } catch {
      case e: Throwable =>
        (None, Some(SpecError(JsonUtil.extractErrorCode(e), Option(e.getMessage).getOrElse(""))), None)
    }

    // If the test author declared `expectError`, the operation MUST throw.
    // See ReadCapture.capture for full semantics.
    expectError.foreach { expected =>
      expectedError match {
        case None =>
          throw new RuntimeException(
            s"Snapshot $specName: declared expectError=" +
              (if (expected.isEmpty) "(any)" else s"'$expected'") +
              " but operation succeeded")
        case Some(err) if expected.nonEmpty &&
            JsonUtil.normalizeErrorCode(err.errorCode) !=
              JsonUtil.normalizeErrorCode(expected) =>
          throw new RuntimeException(
            s"Snapshot $specName: declared expectError='$expected' but got " +
              s"'${err.errorCode}'")
        case _ => // matches
      }
    }

    val specVersion = version.orElse(if (timestamp.isEmpty) resolvedVersion else None)
    val spec = SnapshotSpec(specVersion, timestamp, expected, expectedError)
    JsonUtil.writeSpec(specPath, spec)
    validateFromSpec(spark, tablePath, specPath)

    (expected, expectedError) match {
      case (Some(_), _) =>
        println(s"  Snapshot captured: $specName (version=${resolvedVersion.get})")
      case (_, Some(err)) =>
        println(s"  Snapshot captured (error): $specName [${err.errorCode}] ${err.errorMessage}")
      case _ =>
    }
  }

  def validateFromSpec(spark: SparkSession, tablePath: Path, specPath: Path): Unit = {
    val spec = JsonUtil.readSnapshotSpec(specPath)
    val specName = specPath.getFileName.toString.stripSuffix(".json")
    val harness = DeltaHarness.get

    // Check error case FIRST - if expectedError is defined, this is an error spec
    if (spec.expectedError.isDefined) {
      val err = spec.expectedError.get
      val actualCode = try {
        val log = harness.openLog(spark, tablePath.toString)
        val snapshot = JsonUtil.resolveSnapshot(spark, log, tablePath.toString, spec.version, spec.timestamp)
        if (snapshot.version < 0) Some("DELTA_TABLE_NOT_FOUND") else None
      } catch {
        case e: Throwable => Some(JsonUtil.extractErrorCode(e))
      }
      require(actualCode.isDefined,
        s"Error validation FAILED for $specName: expected operation to fail but it succeeded")
      require(JsonUtil.normalizeErrorCode(actualCode.get) == JsonUtil.normalizeErrorCode(err.errorCode),
        s"Error code mismatch for $specName: captured '${err.errorCode}' but got '${actualCode.get}'")
    } else if (spec.expected.isDefined) {
      val exp = spec.expected.get
      val log = harness.openLog(spark, tablePath.toString)
      val snapshot = JsonUtil.resolveSnapshot(spark, log, tablePath.toString, spec.version, spec.timestamp)
      val actualProtocol = JsonUtil.mapper.treeToValue(
        JsonUtil.mapper.readTree(snapshot.protocolJson).get("protocol"), classOf[Any])
      val actualMetadata = JsonUtil.mapper.treeToValue(
        JsonUtil.mapper.readTree(snapshot.metadataJson).get("metaData"), classOf[Any])

      val expectedProtoJson = JsonUtil.mapper.writeValueAsString(exp.protocol)
      val actualProtoJson = JsonUtil.mapper.writeValueAsString(actualProtocol)
      require(expectedProtoJson == actualProtoJson,
        s"Snapshot validation failed for $specName: protocol mismatch")

      val expectedMetaJson = JsonUtil.mapper.writeValueAsString(exp.metadata)
      val actualMetaJson = JsonUtil.mapper.writeValueAsString(actualMetadata)
      require(expectedMetaJson == actualMetaJson,
        s"Snapshot validation failed for $specName: metadata mismatch")
    }
    // else: neither expected nor expectedError - nothing to validate
  }
}
