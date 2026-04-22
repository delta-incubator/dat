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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import io.delta.workload.deltaharness.DeltaHarness

object ReadCapture {

  private val MaxExpectedDataRows = 5000000L

  def capture(
      spark: SparkSession, testId: String, tablePath: Path,
      outputDir: Path, specsDir: Path, name: String,
      predicate: Option[String] = None, version: Option[Long] = None,
      timestamp: Option[String] = None, columns: Option[Seq[String]] = None,
      expectError: Option[String] = None): Unit = {

    val specName = s"${testId}_$name"
    require(!(version.isDefined && timestamp.isDefined),
      s"Read $specName: cannot specify both version and timestamp")

    val expectedDir = outputDir.resolve("expected").resolve(specName)
    Files.createDirectories(expectedDir)
    val specPath = specsDir.resolve(s"$specName.json")

    val harness = DeltaHarness.get

    val (expected, expectedError, addFilesJson) = try {
      val log = harness.openLog(spark, tablePath.toString)
      val snapshot = JsonUtil.resolveSnapshot(spark, log, tablePath.toString, version, timestamp)

      var df = JsonUtil.buildDeltaReader(spark, tablePath, version, timestamp)
      df = JsonUtil.applyFilters(df, predicate, columns)
      df = df.cache()
      val count = df.count()

      // Get input files from the executed query to determine data skipping
      val inputFilePaths = df.inputFiles.map(_.split("/").last).toSet
      val allAddFiles = snapshot.allFiles.select("path", "json").collect()
      val scannedRows = allAddFiles.filter { r =>
        inputFilePaths.contains(r.getString(0).split("/").last)
      }
      val addFilesJson = scannedRows.map(_.getString(1)).toSeq
      val totalFileCount = allAddFiles.length.toLong

      try {
        writeExpectedData(expectedDir, df, count, specName)
        writeExpectedMetadata(spark, expectedDir, addFilesJson)
        (Some(ReadExpected(count, addFilesJson.length, totalFileCount - addFilesJson.length)), None, addFilesJson)
      } finally {
        df.unpersist()
      }
    } catch {
      case e: Throwable =>
        (None, Some(SpecError(JsonUtil.extractErrorCode(e), Option(e.getMessage).getOrElse(""))), Seq.empty[String])
    }

    // If the test author declared `expectError`, the operation MUST throw.
    // - non-empty expectError → captured error code must match.
    // - empty expectError ("") → any error is fine; just assert one happened.
    // - None → preserve legacy auto-detect behavior (no assertion).
    expectError.foreach { expected =>
      def fail(msg: String): Nothing = {
        // Spark succeeded → wipe any partially-written expected_data so we
        // don't leave a half-baked workload directory behind.
        if (expectedDir.toFile.exists()) FileUtils.deleteDirectory(expectedDir.toFile)
        throw new RuntimeException(msg)
      }
      expectedError match {
        case None =>
          fail(s"Read $specName: declared expectError=" +
            (if (expected.isEmpty) "(any)" else s"'$expected'") +
            " but operation succeeded")
        case Some(err) if expected.nonEmpty &&
            JsonUtil.normalizeErrorCode(err.errorCode) !=
              JsonUtil.normalizeErrorCode(expected) =>
          fail(s"Read $specName: declared expectError='$expected' but got " +
            s"'${err.errorCode}'")
        case _ => // matches
      }
    }

    val spec = ReadSpec(version, timestamp, predicate, columns, expected, expectedError)
    JsonUtil.writeSpec(specPath, spec)
    validateFromSpec(spark, tablePath, expectedDir, specPath)

    (expected, expectedError) match {
      case (Some(exp), _) =>
        println(s"  Read captured: $specName (${exp.rowCount} rows, ${exp.fileCount}/${exp.fileCount + exp.filesSkipped} files)")
      case (_, Some(err)) =>
        println(s"  Read captured (error): $specName [${err.errorCode}] ${err.errorMessage}")
      case _ =>
    }
  }

  private def writeExpectedData(expectedDir: Path, df: DataFrame, count: Long, specName: String): Unit = {
    val path = expectedDir.resolve("expected_data")
    if (path.toFile.exists()) FileUtils.deleteDirectory(path.toFile)
    if (count <= MaxExpectedDataRows) {
      df.write.mode(SaveMode.Overwrite).parquet(path.toString)
    } else {
      System.err.println(s"WARN: Skipping expected_data for $specName: $count rows exceeds limit")
    }
  }

  private def writeExpectedMetadata(spark: SparkSession, expectedDir: Path, addFilesJson: Seq[String]): Unit = {
    val path = expectedDir.resolve("expected_metadata")
    if (path.toFile.exists()) FileUtils.deleteDirectory(path.toFile)
    if (addFilesJson.nonEmpty) {
      spark.createDataset(addFilesJson)(org.apache.spark.sql.Encoders.STRING)
        .toDF("action").write.mode(SaveMode.Overwrite).parquet(path.toString)
    }
  }

  def validateFromSpec(spark: SparkSession, tablePath: Path,
      expectedDir: Path, specPath: Path): Unit = {
    val spec = JsonUtil.readReadSpec(specPath)
    val specName = specPath.getFileName.toString.stripSuffix(".json")

    // Check error case FIRST - if expectedError is defined, this is an error spec
    if (spec.expectedError.isDefined) {
      val err = spec.expectedError.get
      val harness = DeltaHarness.get
      val actualCode = try {
        // Use same code path as capture: openLog -> resolveSnapshot -> build reader.
        // Force a full scan via `noop` write — `count()` alone is answered from
        // the Delta log's stats without touching parquet files, so corrupt-file
        // specs would spuriously succeed.
        val log = harness.openLog(spark, tablePath.toString)
        val _ = JsonUtil.resolveSnapshot(spark, log, tablePath.toString, spec.version, spec.timestamp)
        JsonUtil.applyFilters(
          JsonUtil.buildDeltaReader(spark, tablePath, spec.version, spec.timestamp),
          spec.predicate, spec.columns)
          .write.format("noop").mode("overwrite").save()
        None
      } catch {
        case e: Throwable => Some(JsonUtil.extractErrorCode(e))
      }
      require(actualCode.isDefined,
        s"Error validation FAILED for $specName: expected operation to fail but it succeeded")
      require(JsonUtil.normalizeErrorCode(actualCode.get) == JsonUtil.normalizeErrorCode(err.errorCode),
        s"Error code mismatch for $specName: captured '${err.errorCode}' but got '${actualCode.get}'")
    } else if (spec.expected.isDefined) {
      val rereadDf = JsonUtil.applyFilters(
        JsonUtil.buildDeltaReader(spark, tablePath, spec.version, spec.timestamp),
        spec.predicate, spec.columns)

      val expectedDataPath = expectedDir.resolve("expected_data")
      if (Files.exists(expectedDataPath)) {
        JsonUtil.assertMultisetsEqual(
          JsonUtil.toRowMultiset(spark.read.parquet(expectedDataPath.toString)),
          JsonUtil.toRowMultiset(rereadDf),
          specName)
      }

      val expectedMetaPath = expectedDir.resolve("expected_metadata")
      if (Files.exists(expectedMetaPath)) {
        // Re-derive which AddFiles the current scan would touch, so we can
        // compare against what was captured at generation time.
        val harness = DeltaHarness.get
        val log = harness.openLog(spark, tablePath.toString)
        val snapshot = JsonUtil.resolveSnapshot(
          spark, log, tablePath.toString, spec.version, spec.timestamp)
        val inputFilePaths = rereadDf.inputFiles.map(_.split("/").last).toSet
        val allAddFiles = snapshot.allFiles.select("path", "json").collect()
        val rereadAddFilesJson = allAddFiles
          .filter(r => inputFilePaths.contains(r.getString(0).split("/").last))
          .map(_.getString(1)).toSeq.sorted

        val written = spark.read.parquet(expectedMetaPath.toString)
          .collect().map(_.getString(0)).sorted
        require(written.sameElements(rereadAddFilesJson),
          s"Metadata validation FAILED for $specName")
      }
    }
    // else: neither expected nor expectedError - nothing to validate
  }
}
