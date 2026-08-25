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

package io.delta.workload.validate

import java.nio.file.{Files, Path}
import java.util.concurrent.Executors

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession

import io.delta.workload.capture.{ReadCapture, SnapshotCapture}
import io.delta.workload.json.JsonUtil
import io.delta.workload.model._

/** The outcome of validating a single spec: passed, or failed with a reason. */
sealed trait SpecOutcome { def spec: String }
case class SpecPassed(spec: String) extends SpecOutcome
case class SpecFailed(spec: String, reason: String) extends SpecOutcome

/** Result of validating a workload tree: one [[SpecOutcome]] per spec. */
case class ValidationResult(outcomes: Seq[SpecOutcome]) {
  def failures: Seq[SpecFailed] = outcomes.collect { case f: SpecFailed => f }
  def passed: Int = outcomes.count(_.isInstanceOf[SpecPassed])
  def success: Boolean = failures.isEmpty
  def errors: Seq[String] = failures.map(f => s"${f.spec}: ${f.reason}")
}

/**
 * The validator for read/snapshot specs. Walks a generated workload tree (or one test dir) and
 * validates each captured spec under `specs/` against the captured `delta/` table exactly,
 * dispatching on the `type` field (`read` / `snapshot`).
 *
 * Used both as the presubmit acceptance entry point ([[validateAll]]) and by the generator to
 * self-check a freshly generated test dir ([[validateTestDir]]).
 */
object WorkloadValidator {

  def validateAll(spark: SparkSession, workloadRoot: Path): ValidationResult = {
    require(Files.isDirectory(workloadRoot), s"Not a directory: $workloadRoot")
    val testDirs = listChildren(workloadRoot).filter(Files.isDirectory(_))
    // Each test dir validates independently (its own temp replay tables, path-based table refs),
    // and SparkSession is thread-safe, so validate them concurrently and let Spark's scheduler
    // interleave the per-dir jobs. (DeltaLog's global cache only thrashes under this; commits are
    // per-table, so the result stays correct.)
    val pool = Executors.newFixedThreadPool(
      math.min(8, math.max(1, Runtime.getRuntime.availableProcessors)))
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
    val results =
      try Await.result(Future.sequence(testDirs.map(d => Future(validateTestDir(spark, d)))), Duration.Inf)
      finally pool.shutdown()
    ValidationResult(results.flatMap(_.outcomes))
  }

  /**
   * Validate every read/snapshot spec in one test dir against the captured `delta/` table.
   * Returns pass count + error details.
   */
  def validateTestDir(spark: SparkSession, testDir: Path): ValidationResult = {
    val specsDir = testDir.resolve("specs")
    if (!Files.isDirectory(specsDir)) return ValidationResult(Seq.empty)
    val deltaTable = testDir.resolve("delta")

    val outcomes = mutable.ArrayBuffer[SpecOutcome]()

    // Parse each spec once into the sealed Spec ADT and dispatch by matching on it.
    val parsed = listSpecs(specsDir).flatMap { f =>
      val id = s"${testDir.getFileName}/${f.getFileName.toString.stripSuffix(".json")}"
      try Some(f -> JsonUtil.readSpec(f))
      catch { case e: Throwable => outcomes += SpecFailed(id, e.toString); None }
    }

    // Validate one spec to a single outcome against the captured `delta/` table exactly; its
    // absence means the corpus is incomplete.
    def validateOne(specFile: Path, spec: Spec): SpecOutcome = {
      val bare = specFile.getFileName.toString.stripSuffix(".json")
      val id = s"${testDir.getFileName}/$bare"
      def readOnly(validate: Path => Unit): SpecOutcome =
        if (Files.isDirectory(deltaTable)) { validate(deltaTable); SpecPassed(id) }
        else SpecFailed(id, s"read-only spec but no captured table at $deltaTable")
      try spec match {
        case _: ReadSpec =>
          val expectedDir = testDir.resolve("expected").resolve(bare)
          readOnly(t => ReadCapture.validateFromSpec(spark, t, expectedDir, specFile))
        case _: SnapshotSpec =>
          readOnly(t => SnapshotCapture.validateFromSpec(spark, t, specFile))
      } catch {
        case e: Throwable => SpecFailed(id, e.toString)
      }
    }

    outcomes ++= parsed.map { case (f, spec) => validateOne(f, spec) }
    ValidationResult(outcomes.toSeq)
  }

  private def listSpecs(specsDir: Path): Seq[Path] = {
    val stream = Files.list(specsDir)
    try stream.iterator().asScala.filter(_.getFileName.toString.endsWith(".json")).toSeq.sorted
    finally stream.close()
  }

  private def listChildren(dir: Path): List[Path] = {
    val stream = Files.list(dir)
    try stream.iterator().asScala.toList finally stream.close()
  }
}
