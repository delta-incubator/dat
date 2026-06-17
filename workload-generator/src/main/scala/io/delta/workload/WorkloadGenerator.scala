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

import scala.collection.mutable

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

/**
 * Internal workload generation engine. Use [[WorkloadTestSuite]] as the public API:
 *
 * {{{
 * class DeletionVectorsSuite extends WorkloadTestSuite("deletion_vectors") {
 *   test("dv_delete_basic") {
 *     sql("CREATE TABLE tbl (id INT, name STRING) USING delta " +
 *       "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
 *     sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
 *     sql("DELETE FROM tbl WHERE id = 2")
 *     val t = registerTable("tbl")
 *     readSpec(t)
 *     readSpec(t, version = 0)
 *     readSpec(t, predicate = "id > 1")
 *     snapshotSpec(t)
 *   }
 * }
 * }}}
 */
object WorkloadGenerator {

  private def checkAssertion(config: HasAssertion[_], specPath: Path): Unit = {
    config.assertion.foreach { check =>
      val node = JsonUtil.mapper.readTree(Files.readAllBytes(specPath))
      check(node)
    }
  }

  private[workload] def generateTable(
      spark: SparkSession,
      ts: TableSpec,
      outputBase: Path): WorkloadResult = {
    val dirName = ts.outputName
    val testOutputDir = outputBase.resolve(dirName)

    println(s"--- $dirName ---")

    // Set up output directory
    if (Files.exists(testOutputDir)) FileUtils.deleteDirectory(testOutputDir.toFile)
    Files.createDirectories(testOutputDir)
    val specsDir = testOutputDir.resolve("specs")
    Files.createDirectories(specsDir)
    Files.createDirectories(testOutputDir.resolve("expected"))

    // Copy Delta table. Skip:
    //  - transient files that async engine hooks may be mid-cleaning (e.g.
    //    some engines write `.crc.<uuid>.tmp` files and delete them shortly after;
    //    they can disappear between listing and copying)
    //  - Hadoop CRC sidecars (dot-prefixed `.NAME.crc`): these would go
    //    stale on any subsequent `mutateTable` / `modifyCommitActions` and
    //    cause `ChecksumFileSystem` to reject the (intentionally) corrupted
    //    file. Workloads use `RawLocalFileSystem` for reads anyway, so the
    //    sidecars carry no information for our consumers.
    // NOTE: Delta version-checksums (`_delta_log/NNN.crc`, no dot prefix) are
    // NOT excluded; those are protocol-level artifacts.
    val destTablePath = testOutputDir.resolve("delta")
    val copyFilter: java.io.FileFilter = (f: java.io.File) => {
      val n = f.getName
      val isTransientTmp = n.startsWith(".") && (n.contains(".tmp") || n.endsWith(".tmp"))
      val isHadoopCrc = n.startsWith(".") && n.endsWith(".crc")
      !(isTransientTmp || isHadoopCrc)
    }
    FileUtils.copyDirectory(ts.sourcePath.toFile, destTablePath.toFile, copyFilter)

    // Apply mutations on the copied table
    ts.mutations.foreach { mutate =>
      mutate(destTablePath)
    }

    // When this workload has a write spec, read/snapshot specs point to it (making them
    // write-derived: a consumer reconstructs the table from the write spec before validating).
    val writePtr = ts.writeBuilder.map(_ => s"${dirName}_write.json")

    // Snapshot specs
    val snapshotNames = mutable.ArrayBuffer[String]()
    val explicits = if (ts.snapshotSpecs.isEmpty) {
      Seq(SnapshotSpecConfig(None, None))
    } else ts.snapshotSpecs
    for (ss <- explicits) {
      SnapshotCapture.capture(spark, dirName, destTablePath, specsDir,
        version = ss.version, timestamp = ss.timestamp,
        expectError = ss.expectError, writeSpec = writePtr)
      val specName = (ss.version, ss.timestamp) match {
        case (Some(v), _) => s"${dirName}_snapshot_v$v"
        case (_, Some(t)) =>
          s"${dirName}_snapshot_ts_${t.replace(":", "-").replace(" ", "_")}"
        case _ => s"${dirName}_snapshot"
      }
      if (!snapshotNames.contains(specName)) snapshotNames += specName
      checkAssertion(ss, specsDir.resolve(s"$specName.json"))
    }

    // Read specs
    val readNames = mutable.ArrayBuffer[String]()
    for (rs <- ts.readSpecs) {
      ReadCapture.capture(spark, dirName, destTablePath, testOutputDir, specsDir,
        name = rs.name, predicate = rs.predicate, version = rs.version,
        timestamp = rs.timestamp, columns = rs.columns,
        expectError = rs.expectError, writeSpec = writePtr)
      val specName = s"${dirName}_${rs.name}"
      readNames += specName
      checkAssertion(rs, specsDir.resolve(s"$specName.json"))
    }

    // Write spec - serialize the recorded commit history into specs/ + expected/<name>_write/
    ts.writeBuilder.foreach { builder =>
      builder.buildSpec(spark, destTablePath, testOutputDir)
    }

    // table_info.json - wrap in try/catch for corrupt tables
    try {
      TableInfoWriter.write(spark, destTablePath, testOutputDir,
        name = dirName, description = ts.description, tags = ts.tags)
    } catch {
      case e: Throwable =>
        System.err.println(s"WARN: Could not write table_info.json for $dirName: ${e.getMessage}")
    }

    // Self-validate the write workload: the single WorkloadValidator replays the write spec into
    // a fresh table and re-checks the write-derived read/snapshot specs against it.
    ts.writeBuilder.foreach { _ =>
      val result = WorkloadValidator.validateTestDir(spark, testOutputDir)
      require(result.success,
        s"Write spec validation FAILED for $dirName:\n  ${result.errors.mkString("\n  ")}")
    }

    // Repro placeholder
    val reproDir = testOutputDir.resolve("repro")
    Files.createDirectories(reproDir)
    Files.write(reproDir.resolve("generate.scala"),
      s"// Generated by ${ts.outputName} test\n".getBytes("UTF-8"))

    val total = snapshotNames.size + readNames.size
    println(s"  $dirName: $total specs")

    WorkloadResult(testOutputDir.toString, dirName, total,
      readNames.toSeq, snapshotNames.toSeq)
  }

}

private[workload] trait HasAssertion[T] {
  var assertion: Option[com.fasterxml.jackson.databind.JsonNode => Unit] = None
  def deserialize: com.fasterxml.jackson.databind.JsonNode => T
}

// ---------------------------------------------------------------------------
// Internal data structures: the model the DSL builds and the engine consumes
// ---------------------------------------------------------------------------

private[workload] class TableSpec(
    private var _outputName: String,
    val description: String,
    val tags: Seq[String],
    val sourcePath: Path) {
  def outputName: String = _outputName
  /** Set once by the orchestrator after body execution. */
  private[workload] def resolveOutputName(name: String): Unit = { _outputName = name }
  val readSpecs = mutable.ArrayBuffer[ReadSpecConfig]()
  val snapshotSpecs = mutable.ArrayBuffer[SnapshotSpecConfig]()
  val mutations = mutable.ArrayBuffer[Path => Unit]()
  var writeBuilder: Option[WriteSpecBuilder] = None
}

case class WorkloadResult(
    outputDir: String,
    testId: String,
    specsGenerated: Int,
    readSpecs: Seq[String],
    snapshotSpecs: Seq[String])

private[workload] case class ReadSpecConfig(
    name: String, predicate: Option[String], version: Option[Long],
    timestamp: Option[String], columns: Option[Seq[String]],
    expectError: Option[String] = None) extends HasAssertion[ReadSpec] {
  val deserialize = (n: com.fasterxml.jackson.databind.JsonNode) =>
    JsonUtil.mapper.treeToValue(n, classOf[ReadSpec])
}

private[workload] case class SnapshotSpecConfig(
    version: Option[Long], timestamp: Option[String],
    expectError: Option[String] = None) extends HasAssertion[SnapshotSpec] {
  val deserialize = (n: com.fasterxml.jackson.databind.JsonNode) =>
    JsonUtil.mapper.treeToValue(n, classOf[SnapshotSpec])
}
