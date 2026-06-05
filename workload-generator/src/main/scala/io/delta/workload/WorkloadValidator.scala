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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.SparkSession

/** Result of validating a workload tree: per-spec pass/fail tallies + error details. */
case class ValidationResult(passed: Int, errors: Seq[String]) {
  def success: Boolean = errors.isEmpty
}

/**
 * Walks a generated workload tree and re-validates every captured spec against
 * the tables on disk. Used as the presubmit acceptance-test entry point: given
 * a committed tarball extracted locally, verify that the current Delta
 * implementation can still reproduce the captured specifications.
 *
 * Expected tree layout (produced by [[WorkloadGenerator]]):
 * {{{
 *   <workloadRoot>/<test_case>/
 *     delta/                    # the Delta table
 *     specs/<spec_name>.json    # flat — dispatched by the "type" field
 *     expected/<spec_name>/
 *       expected_data           # parquet
 *       expected_metadata       # parquet
 * }}}
 *
 * Each `<test_case>/delta/` is opened and each spec under `<test_case>/specs/`
 * is replayed via [[ReadCapture.validateFromSpec]] or
 * [[SnapshotCapture.validateFromSpec]] based on the spec's `type` field.
 * A missing `delta/` is skipped (corruption tests may omit the table).
 */
object WorkloadValidator {

  def validateAll(spark: SparkSession, workloadRoot: Path): ValidationResult = {
    require(Files.isDirectory(workloadRoot), s"Not a directory: $workloadRoot")

    val errors = mutable.ArrayBuffer[String]()
    var passed = 0

    val testDirs = listChildren(workloadRoot).filter(Files.isDirectory(_))

    for (testDir <- testDirs) {
      val tablePath = testDir.resolve("delta")
      val specsDir = testDir.resolve("specs")
      if (!Files.isDirectory(tablePath) || !Files.isDirectory(specsDir)) {
        // Nothing to validate — corruption tests may legitimately omit these.
      } else {
        for (specPath <- listChildren(specsDir).filter(_.getFileName.toString.endsWith(".json"))) {
          val specName = specPath.getFileName.toString.stripSuffix(".json")
          val expectedDir = testDir.resolve("expected").resolve(specName)
          try {
            dispatch(spark, tablePath, expectedDir, specPath)
            passed += 1
          } catch {
            case e: Throwable =>
              errors += s"${testDir.getFileName}/$specName: ${e.getMessage}"
          }
        }
      }
    }

    ValidationResult(passed, errors.toSeq)
  }

  /** Dispatch to the correct validator based on the spec's `type` field. */
  private def dispatch(
      spark: SparkSession, tablePath: Path, expectedDir: Path, specPath: Path): Unit = {
    val node = JsonUtil.mapper.readTree(Files.readAllBytes(specPath))
    Option(node.get("type")).map(_.asText()).getOrElse("") match {
      case "read"     => ReadCapture.validateFromSpec(spark, tablePath, expectedDir, specPath)
      case "snapshot" => SnapshotCapture.validateFromSpec(spark, tablePath, specPath)
      case other      => throw new IllegalArgumentException(
        s"Unknown spec type '$other' in ${specPath.getFileName}")
    }
  }

  private def listChildren(dir: Path): List[Path] = {
    val stream = Files.list(dir)
    try stream.iterator().asScala.toList finally stream.close()
  }
}
