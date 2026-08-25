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

import io.delta.workload.capture.{ReadCapture, SnapshotCapture, TableInfoCapture}
import io.delta.workload.json.JsonUtil
import io.delta.workload.model._

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
      ts: TableDecl,
      outputBase: Path): String = {
    val dirName = ts.outputName
    val testOutputDir = outputBase.resolve(dirName)

    println(s"--- $dirName ---")

    if (Files.exists(testOutputDir)) FileUtils.deleteDirectory(testOutputDir.toFile)
    Files.createDirectories(testOutputDir)
    val specsDir = testOutputDir.resolve("specs")
    Files.createDirectories(specsDir)
    Files.createDirectories(testOutputDir.resolve("expected"))

    // Copy the Delta table, skipping two kinds of file:
    //  - transient `.crc.<uuid>.tmp` files some engines write and delete shortly after, which can
    //    disappear between listing and copying.
    //  - Hadoop CRC sidecars (dot-prefixed `.NAME.crc`), which go stale on any `mutateTable` /
    //    `modifyCommitActions` and make `ChecksumFileSystem` reject the corrupted file; workloads
    //    read via `RawLocalFileSystem`, so the sidecars carry nothing for consumers.
    // Delta version-checksums (`_delta_log/NNN.crc`, no dot prefix) are kept: those are
    // protocol-level artifacts.
    val destTablePath = testOutputDir.resolve("delta")
    val copyFilter: java.io.FileFilter = (f: java.io.File) => {
      val n = f.getName
      val isTransientTmp = n.startsWith(".") && (n.contains(".tmp") || n.endsWith(".tmp"))
      val isHadoopCrc = n.startsWith(".") && n.endsWith(".crc")
      !(isTransientTmp || isHadoopCrc)
    }
    FileUtils.copyDirectory(ts.sourcePath.toFile, destTablePath.toFile, copyFilter)

    ts.mutations.foreach { mutate =>
      mutate(destTablePath)
    }

    val snapshotNames = mutable.ArrayBuffer[String]()
    val explicits = if (ts.snapshotSpecs.isEmpty) {
      Seq(SnapshotSpecConfig(SnapshotQuery()))
    } else ts.snapshotSpecs
    for (ss <- explicits) {
      SnapshotCapture.capture(spark, dirName, destTablePath, specsDir,
        query = ss.query, expectError = ss.expectError)
      val specName = (ss.query.version, ss.query.timestamp) match {
        case (Some(v), _) => s"${dirName}_snapshot_v$v"
        case (_, Some(ts)) =>
          s"${dirName}_snapshot_ts_${ts.replace(":", "-").replace(" ", "_")}"
        case _ => s"${dirName}_snapshot"
      }
      if (!snapshotNames.contains(specName)) snapshotNames += specName
      checkAssertion(ss, specsDir.resolve(s"$specName.json"))
    }

    val readNames = mutable.ArrayBuffer[String]()
    for (rs <- ts.readSpecs) {
      ReadCapture.capture(spark, dirName, destTablePath, testOutputDir, specsDir,
        name = rs.name, query = rs.query, expectError = rs.expectError)
      val specName = s"${dirName}_${rs.name}"
      readNames += specName
      checkAssertion(rs, specsDir.resolve(s"$specName.json"))
    }

    // table_info.json. An intentionally-mutated table (corruption / deleted-log tests) may be
    // unreadable and so can't produce it; an unmutated table that fails here is a real bug, so
    // only a mutated table tolerates the failure.
    try {
      TableInfoCapture.write(spark, destTablePath, testOutputDir,
        name = dirName, description = ts.description, tags = ts.tags)
    } catch {
      case e: Throwable if ts.mutations.nonEmpty =>
        System.err.println(s"WARN: skipping table_info.json for mutated table $dirName: ${e.getMessage}")
    }

    // Repro placeholder
    val reproDir = testOutputDir.resolve("repro")
    Files.createDirectories(reproDir)
    Files.write(reproDir.resolve("generate.scala"),
      s"// Generated by ${ts.outputName} test\n".getBytes("UTF-8"))

    val total = snapshotNames.size + readNames.size
    println(s"  $dirName: $total specs")

    dirName
  }

}
