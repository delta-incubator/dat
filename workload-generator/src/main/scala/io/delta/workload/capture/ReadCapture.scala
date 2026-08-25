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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import io.delta.workload.deltaharness.DeltaHarness
import io.delta.workload.engine.{RowComparison, SnapshotResolver, SpecOutcome}
import io.delta.workload.json.JsonUtil
import io.delta.workload.model._

object ReadCapture {

  private val MaxExpectedDataRows = 5000000L

  def capture(
      spark: SparkSession, testId: String, tablePath: Path,
      outputDir: Path, specsDir: Path, name: String,
      query: ReadQuery = ReadQuery(),
      expectError: ErrorExpectation = AutoDetect): Unit = {

    val specName = s"${testId}_$name"
    require(!(query.version.isDefined && query.timestamp.isDefined),
      s"Read $specName: cannot specify both version and timestamp")

    val expectedDir = outputDir.resolve("expected").resolve(specName)
    Files.createDirectories(expectedDir)
    val specPath = specsDir.resolve(s"$specName.json")

    val harness = DeltaHarness.get

    val expectation: SpecExpectation[ReadResult] = SpecOutcome.captureExpectation {
      val log = harness.openLog(spark, tablePath.toString)
      val snapshot =
        SnapshotResolver.resolveSnapshot(spark, log, tablePath.toString, query.version, query.timestamp)

      var df = SnapshotResolver.buildDeltaReader(spark, tablePath, query.version, query.timestamp)
      df = SnapshotResolver.applyFilters(df, query.predicate, query.columns)
      // Resolve scanned files from the uncached plan: a cached DataFrame reports no inputFiles,
      // and validation re-derives them the same (uncached) way.
      val inputFilePaths = df.inputFiles.map(_.split("/").last).toSet
      df = df.cache()
      val count = df.count()
      val allAddFiles = snapshot.allFiles.select("path", "json").collect()
      val scannedRows = allAddFiles.filter { r =>
        inputFilePaths.contains(r.getString(0).split("/").last)
      }
      val addFilesJson = scannedRows.map(_.getString(1)).toSeq
      val totalFileCount = allAddFiles.length.toLong

      try {
        writeExpectedData(expectedDir, df, count, specName)
        writeExpectedMetadata(spark, expectedDir, addFilesJson)
        Succeeded(ReadResult(count, addFilesJson.length, totalFileCount - addFilesJson.length))
      } finally {
        df.unpersist()
      }
    }

    // If an error was declared but Spark succeeded, wipe the half-written expected_data first.
    SpecOutcome.assertExpectation(expectation, specName, expectError) {
      if (expectedDir.toFile.exists()) FileUtils.deleteDirectory(expectedDir.toFile)
    }

    val spec = ReadSpec(query, expectation)
    JsonUtil.writeSpec(specPath, spec)
    validateFromSpec(spark, tablePath, expectedDir, specPath)

    expectation match {
      case Succeeded(exp) =>
        println(s"  Read captured: $specName (${exp.rowCount} rows, ${exp.fileCount}/${exp.fileCount + exp.filesSkipped} files)")
      case Failed(err) =>
        println(s"  Read captured (error): $specName [${err.errorCode}] ${err.errorMessage}")
    }
  }

  private def writeExpectedData(expectedDir: Path, df: DataFrame, count: Long, specName: String): Unit = {
    require(count <= MaxExpectedDataRows,
      s"$specName: result has $count rows, over the expected_data limit of $MaxExpectedDataRows. " +
        "Exact-content validation needs every row materialized: narrow the spec or raise the limit.")
    val path = expectedDir.resolve("expected_data")
    if (path.toFile.exists()) FileUtils.deleteDirectory(path.toFile)
    df.write.mode(SaveMode.Overwrite).parquet(path.toString)
  }

  private def writeExpectedMetadata(spark: SparkSession, expectedDir: Path, addFilesJson: Seq[String]): Unit = {
    val path = expectedDir.resolve("expected_metadata")
    if (path.toFile.exists()) FileUtils.deleteDirectory(path.toFile)
    // Always write, even when no files were scanned, so a missing dir at validation time
    // unambiguously signals a bug; a zero-scan spec still has an (empty) dir.
    spark.createDataset(addFilesJson)(org.apache.spark.sql.Encoders.STRING)
      .toDF("action").write.mode(SaveMode.Overwrite).parquet(path.toString)
  }

  def validateFromSpec(spark: SparkSession, tablePath: Path,
      expectedDir: Path, specPath: Path, checkMetadata: Boolean = true): Unit = {
    val spec = JsonUtil.readReadSpec(specPath)
    val specName = specPath.getFileName.toString.stripSuffix(".json")
    val harness = DeltaHarness.get

    SpecOutcome.compareExpectation(spec.expectation, specName) {
      // error spec: force a full scan via a `noop` write. `count()` alone is answered from the Delta
      // log's stats without touching parquet, so corrupt-file specs would spuriously succeed. The
      // thrown error's code is returned for comparison; None means it succeeded.
      SpecOutcome.runErrorCode {
        val log = harness.openLog(spark, tablePath.toString)
        SnapshotResolver.resolveSnapshot(spark, log, tablePath.toString, spec.query.version, spec.query.timestamp)
        SnapshotResolver.applyFilters(
          SnapshotResolver.buildDeltaReader(spark, tablePath, spec.query.version, spec.query.timestamp),
          spec.query.predicate, spec.query.columns)
          .write.format("noop").mode("overwrite").save()
        None
      }
    } { _ => // success: validate the rows (and, unless write-derived, the scanned files)
      val rereadDf = SnapshotResolver.applyFilters(
        SnapshotResolver.buildDeltaReader(spark, tablePath, spec.query.version, spec.query.timestamp),
        spec.query.predicate, spec.query.columns)

      val expectedDataPath = expectedDir.resolve("expected_data")
      require(Files.exists(expectedDataPath),
        s"Validation FAILED for $specName: expected_data is missing")
      RowComparison.assertRowsEqual(
        spark.read.parquet(expectedDataPath.toString), rereadDf, specName)

      // checkMetadata is false only for write-derived specs: a replayed table has different
      // physical files than capture, so its scanned-file set cannot match by construction.
      if (checkMetadata) {
          val expectedMetaPath = expectedDir.resolve("expected_metadata")
          require(Files.exists(expectedMetaPath),
            s"Validation FAILED for $specName: expected_metadata is missing")
          // Re-derive which AddFiles the current scan would touch, so we can
          // compare against what was captured at generation time.
          val harness = DeltaHarness.get
          val log = harness.openLog(spark, tablePath.toString)
          val snapshot = SnapshotResolver.resolveSnapshot(
            spark, log, tablePath.toString, spec.query.version, spec.query.timestamp)
          val inputFilePaths = rereadDf.inputFiles.map(_.split("/").last).toSet
          val allAddFiles = snapshot.allFiles.select("path", "json").collect()
          val rereadAddFilesJson = allAddFiles
            .filter(r => inputFilePaths.contains(r.getString(0).split("/").last))
            .map(_.getString(1)).toSeq.sorted

          val written = spark.read.schema("action STRING").parquet(expectedMetaPath.toString)
            .collect().map(_.getString(0)).sorted
          require(written.sameElements(rereadAddFilesJson),
            s"Metadata validation FAILED for $specName: captured ${written.length} scanned files, " +
              s"re-derived ${rereadAddFilesJson.length} (mismatch in which AddFiles the scan touched)")
        }
    }
  }
}
