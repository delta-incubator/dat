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
import scala.util.control.NonFatal

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import io.delta.workload.capture.{ReadCapture, SnapshotCapture}
import io.delta.workload.json.JsonUtil
import io.delta.workload.model._
import io.delta.workload.write.WriteReplay

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
 * The single validator for every spec type. Walks a generated workload tree (or one test dir)
 * and validates each captured spec under `specs/`, dispatching on the `type` field:
 *
 *   - `write`: replay its `commits` into a fresh table, compare rows to `expected/<name>/`.
 *   - `read` / `snapshot`: the captured-vs-replayed target is chosen once per directory. If the
 *     directory has a write spec, every read/snapshot spec validates against the table replayed
 *     from it (a read by rows only; a snapshot leniently, since a replay mints fresh per-table
 *     ids). Otherwise it is read-only: validate against the captured `delta/` table exactly.
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
   * Validate every spec in one test dir. Write-derived read/snapshot specs and the write spec
   * share a single replayed table per write spec (memoized). Returns pass count + error details.
   */
  def validateTestDir(spark: SparkSession, testDir: Path): ValidationResult = {
    val specsDir = testDir.resolve("specs")
    if (!Files.isDirectory(specsDir)) return ValidationResult(Seq.empty)
    val deltaTable = testDir.resolve("delta")

    val outcomes = mutable.ArrayBuffer[SpecOutcome]()
    val replays = mutable.Map[String, Path]() // write-spec file name -> replayed table path
    val tempDirs = mutable.ArrayBuffer[Path]()
    def replayOf(writeSpecName: String): Path = replays.getOrElseUpdate(writeSpecName, {
      val td = Files.createTempDirectory("wl_replay")
      tempDirs += td
      val table = td.resolve("replay_table")
      WriteReplay.replayInto(spark, testDir, specsDir.resolve(writeSpecName), table)
      table
    })

    // Parse each spec once into the sealed Spec ADT and dispatch by matching on it.
    val parsed = listSpecs(specsDir).flatMap { f =>
      val id = s"${testDir.getFileName}/${f.getFileName.toString.stripSuffix(".json")}"
      try Some(f -> JsonUtil.readSpec(f))
      catch { case e: Throwable => outcomes += SpecFailed(id, e.toString); None }
    }

    // Captured-vs-replayed is decided per directory, not per spec: if a write spec is present, every
    // read/snapshot spec validates against the table replayed from it (leniently, since a replay
    // mints fresh per-table ids); otherwise against the captured `delta/` table (exactly).
    val writeSpecFile = parsed.collectFirst { case (f, _: WriteSpec) => f }
    val writeDerived = writeSpecFile.isDefined
    def queryTarget: Path = writeSpecFile.map(wf => replayOf(wf.getFileName.toString)).getOrElse(deltaTable)

    // Validate one spec to a single outcome. Read-only specs (no write spec in the dir) validate
    // against the captured `delta/` table exactly; its absence means the corpus is incomplete.
    def validateOne(specFile: Path, spec: Spec): SpecOutcome = {
      val bare = specFile.getFileName.toString.stripSuffix(".json")
      val id = s"${testDir.getFileName}/$bare"
      def readOnly(validate: Path => Unit): SpecOutcome =
        if (Files.isDirectory(deltaTable)) { validate(deltaTable); SpecPassed(id) }
        else SpecFailed(id, s"read-only spec but no captured table at $deltaTable")
      try spec match {
        case _: WriteSpec =>
          // Basic validation: triggering the replay (queryTarget) reconstructs the table and
          // asserts replay succeeds + finalVersion == commits.size-1. Row content is checked by
          // the baseline 'latest' read spec; per-version metadata by the snapshot spec.
          val _ = queryTarget; SpecPassed(id)
        case _: ReadSpec =>
          val expectedDir = testDir.resolve("expected").resolve(bare)
          if (writeDerived) { // rows-only against the replayed table
            ReadCapture.validateFromSpec(spark, queryTarget, expectedDir, specFile, checkMetadata = false)
            SpecPassed(id)
          } else readOnly(t => ReadCapture.validateFromSpec(spark, t, expectedDir, specFile))
        case _: SnapshotSpec =>
          if (writeDerived) {
            SnapshotCapture.validateFromSpec(spark, queryTarget, specFile, isWriteValidation = true); SpecPassed(id)
          } else readOnly(t => SnapshotCapture.validateFromSpec(spark, t, specFile))
      } catch {
        case e: Throwable => SpecFailed(id, e.toString)
      }
    }

    try outcomes ++= parsed.map { case (f, spec) => validateOne(f, spec) }
    finally tempDirs.foreach(td => try FileUtils.deleteDirectory(td.toFile) catch { case NonFatal(_) => })
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
