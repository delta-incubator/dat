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

import scala.util.control.NonFatal

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import io.delta.workload.deltaharness.DeltaHarness
import io.delta.workload.engine.{RowComparison, SnapshotResolver, SpecOutcome}
import io.delta.workload.json.JsonUtil
import io.delta.workload.model.{CdfExpected, CdfSpec, SpecError}

object CdfCapture {

  def capture(
      spark: SparkSession, testId: String, tablePath: Path,
      outputDir: Path, specsDir: Path, name: String,
      startVersion: Option[Long] = None, endVersion: Option[Long] = None,
      startTimestamp: Option[String] = None, endTimestamp: Option[String] = None,
      predicate: Option[String] = None, columns: Option[Seq[String]] = None,
      expectError: Option[String] = None): Path = {

    val specName = s"${testId}_$name"
    require(startVersion.isDefined || startTimestamp.isDefined,
      s"CDF $specName: a start bound (startVersion or startTimestamp) is required")
    require(!(startVersion.isDefined && startTimestamp.isDefined),
      s"CDF $specName: cannot specify both startVersion and startTimestamp")
    require(!(endVersion.isDefined && endTimestamp.isDefined),
      s"CDF $specName: cannot specify both endVersion and endTimestamp")

    val expectedDir = outputDir.resolve("expected").resolve(specName)
    Files.createDirectories(expectedDir)
    val specPath = specsDir.resolve(s"$specName.json")

    val (expected, expectedError) = try {
      var df = buildReader(spark, tablePath, startVersion, endVersion,
        startTimestamp, endTimestamp)
      df = SnapshotResolver.applyFilters(df, predicate, columns)
      df = df.drop("_commit_timestamp")
      df = df.cache()
      val count = df.count()

      try {
        writeExpectedData(expectedDir, df)
        (Some(CdfExpected(count)), None)
      } finally {
        df.unpersist()
      }
    } catch {
      case NonFatal(e) =>
        (None, Some(SpecError(SpecOutcome.extractErrorCode(e), Option(e.getMessage).getOrElse(""))))
    }

    // If the test author declared `expectError`, the operation MUST throw.
    // See ReadCapture.capture for full semantics.
    expectError.foreach { expected =>
      def fail(msg: String): Nothing = {
        if (expectedDir.toFile.exists()) FileUtils.deleteDirectory(expectedDir.toFile)
        throw new RuntimeException(msg)
      }
      expectedError match {
        case None =>
          fail(s"CDF $specName: declared expectError=" +
            (if (expected.isEmpty) "(any)" else s"'$expected'") +
            " but operation succeeded")
        case Some(err) if expected.nonEmpty &&
            SpecOutcome.normalizeErrorCode(err.errorCode) !=
              SpecOutcome.normalizeErrorCode(expected) =>
          fail(s"CDF $specName: declared expectError='$expected' but got " +
            s"'${err.errorCode}'")
        case _ => // matches
      }
    }

    val spec = CdfSpec(startVersion, endVersion, startTimestamp, endTimestamp,
      predicate, columns, expected, expectedError)
    JsonUtil.writeSpec(specPath, spec)
    validateFromSpec(spark, tablePath, expectedDir, specPath)

    (expected, expectedError) match {
      case (Some(exp), _) =>
        println(s"  CDF captured: $specName (${exp.rowCount} change rows)")
      case (_, Some(err)) =>
        println(s"  CDF captured (error): $specName [${err.errorCode}] ${err.errorMessage}")
      case _ =>
    }
    specPath
  }

  def validateFromSpec(spark: SparkSession, tablePath: Path,
      expectedDir: Path, specPath: Path): Unit = {
    val spec = JsonUtil.readSpecAs(specPath, classOf[CdfSpec])
    val specName = specPath.getFileName.toString.stripSuffix(".json")

    def reader = SnapshotResolver.applyFilters(
      buildReader(spark, tablePath, spec.startVersion, spec.endVersion,
        spec.startTimestamp, spec.endTimestamp),
      spec.predicate, spec.columns).drop("_commit_timestamp")

    if (spec.expectedError.isDefined) {
      val actualCode = try {
        reader.write.format("noop").mode("overwrite").save()
        None
      } catch {
        case NonFatal(e) => Some(SpecOutcome.extractErrorCode(e))
      }
      require(actualCode.isDefined,
        s"Error validation FAILED for $specName: expected operation to fail but it succeeded")
    } else if (spec.expected.isDefined) {
      val rereadDf = reader

      val expectedDataPath = expectedDir.resolve("expected_data")
      require(Files.exists(expectedDataPath),
        s"Validation FAILED for $specName: expected_data is missing")
      RowComparison.assertRowsEqual(
        spark.read.parquet(expectedDataPath.toString), rereadDf, specName)
    }
  }

  private def writeExpectedData(expectedDir: Path, df: DataFrame): Unit = {
    val path = expectedDir.resolve("expected_data")
    if (path.toFile.exists()) FileUtils.deleteDirectory(path.toFile)
    df.write.mode(SaveMode.Overwrite).parquet(path.toString)
  }

  /**
   * Build the change-feed reader, resolving the table's latest version through the
   * neutral [[DeltaHarness]] SPI so an open-ended range stays bounded.
   */
  private def buildReader(spark: SparkSession, tablePath: Path,
      startVersion: Option[Long], endVersion: Option[Long],
      startTimestamp: Option[String], endTimestamp: Option[String]): DataFrame = {
    val latestVersion = DeltaHarness.get.openLog(spark, tablePath.toString).update().version
    SnapshotResolver.buildCdfReader(spark, tablePath, startVersion, endVersion,
      startTimestamp, endTimestamp, latestVersion)
  }
}
