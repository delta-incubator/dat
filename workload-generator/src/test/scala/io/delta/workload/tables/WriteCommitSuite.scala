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

package io.delta.workload.tables

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

import io.delta.workload.{AddDomainMetadata, AddFileAction, AppTxn, JsonUtil,
  RemoveFileAction, TableHandle, WorkloadTestSuite}

/**
 * Low-level `commit` write workloads. Each case drives [[commitOp]] to produce a real
 * `_delta_log` entry, asserts the produced action independently of replay, and relies on
 * the generator's round-trip validation (replay + re-check specs) for end-to-end coverage.
 */
class WriteCommitSuite extends WorkloadTestSuite("write_commit") {

  /** Active add-file relative paths in the live table's latest version. */
  private def liveAddPaths(t: TableHandle): Seq[String] = {
    val log = io.delta.workload.deltaharness.DeltaHarness.get.openLog(spark, t.sourcePath.toString)
    log.update().allFiles.select("path").collect().map(_.getString(0)).toSeq
  }

  /** Action inner-objects of a given type in a commit's `_delta_log/NNN.json`. */
  private def actionsAt(t: TableHandle, version: Long, action: String): Seq[com.fasterxml.jackson.databind.JsonNode] = {
    val commitFile = t.sourcePath.resolve("_delta_log").resolve(f"$version%020d.json")
    Files.readAllLines(commitFile).asScala.toSeq
      .filter(_.trim.nonEmpty)
      .map(JsonUtil.mapper.readTree)
      .flatMap(n => Option(n.get(action)))
  }

  private def latestVersion(t: TableHandle): Long =
    io.delta.workload.deltaharness.DeltaHarness.get.openLog(spark, t.sourcePath.toString)
      .update().version

  test("commit_with_txn") {
    val w = createTableOp("tbl", schema = "id INT, name STRING")
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    commitOp(w, txn = Some(AppTxn("app-1", 7L)))
    val t = registerWriteSpec(w)

    val v = latestVersion(t)
    val txns = actionsAt(t, v, "txn")
    assert(txns.size == 1, s"expected one txn action at v$v, got ${txns.size}")
    assert(txns.head.get("appId").asText() == "app-1")
    assert(txns.head.get("version").asLong() == 7L)

    readSpec(t, name = "read_all")
    snapshotSpec(t)
  }

  test("commit_with_domain_metadata") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING",
      properties = Map("delta.feature.domainMetadata" -> "supported"))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    commitOp(w, addDomainMetadata = Some(Seq(
      AddDomainMetadata("delta.test", """{"k":"v"}"""))))
    val t = registerWriteSpec(w)

    val v = latestVersion(t)
    val dms = actionsAt(t, v, "domainMetadata")
    assert(dms.size == 1, s"expected one domainMetadata action at v$v, got ${dms.size}")
    assert(dms.head.get("domain").asText() == "delta.test")
    assert(dms.head.get("configuration").asText() == """{"k":"v"}""")
    assert(!dms.head.get("removed").asBoolean())

    readSpec(t, name = "read_all")
    snapshotSpec(t)
  }

  test("commit_add_file") {
    val w = createTableOp("tbl", schema = "id INT, name STRING")
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    val t0 = registerWriteSpec(w)
    // Add a second data file via a low-level commit. Its bytes come from the existing insert
    // file, but commitOp copies them to a fresh in-table path, so the table now holds two
    // adds (the alice row appears twice).
    val source = liveAddPaths(t0).head
    commitOp(w, addFiles = Some(Seq(AddFileAction(dataFile = source))))
    val t = registerWriteSpec(w)

    val v = latestVersion(t)
    val adds = actionsAt(t, v, "add")
    assert(adds.size == 1, s"expected one add action at v$v, got ${adds.size}")

    readSpec(t, name = "read_all")
    snapshotSpec(t)
  }

  test("commit_remove_file") {
    val w = createTableOp("tbl", schema = "id INT, name STRING")
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    val t0 = registerWriteSpec(w)
    // Add a low-level file (deterministic, capture-assigned path), then tombstone it. Removing
    // a low-level-added file (rather than an insert file) keeps the path stable across replay.
    val source = liveAddPaths(t0).head
    commitOp(w, addFiles = Some(Seq(AddFileAction(dataFile = source))))
    val added = actionsAt(t0, latestVersion(t0), "add").head.get("path").asText()
    commitOp(w, removeFiles = Some(Seq(RemoveFileAction(path = added))))
    val t = registerWriteSpec(w)

    val v = latestVersion(t)
    val removes = actionsAt(t, v, "remove")
    assert(removes.size == 1, s"expected one remove action at v$v, got ${removes.size}")
    assert(removes.head.get("path").asText() == added)

    readSpec(t, name = "read_after_remove")
    snapshotSpec(t)
  }
}
