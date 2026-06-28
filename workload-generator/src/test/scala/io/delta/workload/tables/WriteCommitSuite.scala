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

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.types._

import io.delta.workload.{TableHandle, WorkloadTestSuite}
import io.delta.workload.json.JsonUtil
import io.delta.workload.model.{AddDomainMetadata, AddFileInput, AppTxn, WriteCommit}

/**
 * Low-level `commit` write workloads. Each case drives [[commitOp]] to produce a real `_delta_log`
 * entry, asserts the produced action independently of replay, and relies on the generator's
 * round-trip validation (replay + re-check specs) for end-to-end coverage. Low-level adds supply
 * LOGICAL rows; the engine writes the files (paths/stats/partitionValues are engine-derived).
 */
class WriteCommitSuite extends WorkloadTestSuite("write_commit") {

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
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("name", StringType))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    commitOp(w, txn = Some(AppTxn("app-1", 7L)))
    val t = endWrite(w)

    val v = latestVersion(t)
    val txns = actionsAt(t, v, "txn")
    assert(txns.size == 1, s"expected one txn action at v$v, got ${txns.size}")
    assert(txns.head.get("appId").asText() == "app-1")
    assert(txns.head.get("version").asLong() == 7L)

    readSpec(t)
    snapshotSpec(t)
  }

  test("commit_with_domain_metadata") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("name", StringType),
      properties = Map("delta.feature.domainMetadata" -> "supported"))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    commitOp(w, addDomainMetadata = Some(Seq(
      AddDomainMetadata("delta.test", """{"k":"v"}"""))))
    val t = endWrite(w)

    val v = latestVersion(t)
    val dms = actionsAt(t, v, "domainMetadata")
    assert(dms.size == 1, s"expected one domainMetadata action at v$v, got ${dms.size}")
    assert(dms.head.get("domain").asText() == "delta.test")
    assert(dms.head.get("configuration").asText() == """{"k":"v"}""")
    assert(!dms.head.get("removed").asBoolean())

    readSpec(t)
    snapshotSpec(t)
  }

  test("commit_add_file") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("name", StringType))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    // Add a second data file via a low-level commit; the engine writes it and assigns the path.
    commitOp(w, addFiles = Some(Seq(AddFileInput(rows = Seq(Map("id" -> 2, "name" -> "bob"))))))
    val t = endWrite(w)

    val v = latestVersion(t)
    assert(actionsAt(t, v, "add").size == 1, s"expected one add action at v$v")

    readSpec(t)
    snapshotSpec(t)
  }

  test("commit_remove_file") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("name", StringType))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    // Add a file via a low-level commit, then tombstone it by referencing that commit's ordinal.
    val added = commitOp(w, addFiles = Some(Seq(AddFileInput(rows = Seq(Map("id" -> 2, "name" -> "bob"))))))
    val t = endWrite(w)
    val addedPath = actionsAt(t, added.value.toLong, "add").head.get("path").asText()
    commitOp(w, removeFiles = Some(Seq(added)))

    val v = latestVersion(t)
    val removes = actionsAt(t, v, "remove")
    assert(removes.size == 1, s"expected one remove action at v$v, got ${removes.size}")
    assert(removes.head.get("path").asText() == addedPath, "remove should tombstone the added file")

    readSpec(t, name = Some("read_after_remove"))
    snapshotSpec(t)
  }

  test("commit_add_file_with_stats") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("name", StringType))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    // Stats are engine-computed from the rows (numRecords + per-column min/max/nullCount).
    commitOp(w, addFiles = Some(Seq(AddFileInput(
      rows = Seq(Map("id" -> 2, "name" -> "bob"), Map("id" -> 3, "name" -> "cara"))))))
    val t = endWrite(w)

    val v = latestVersion(t)
    val add = actionsAt(t, v, "add").head
    val stats = JsonUtil.mapper.readTree(add.get("stats").asText())
    assert(stats.get("numRecords").asLong() == 2, s"stats: ${add.get("stats").asText()}")
    assert(stats.get("minValues").get("id").asInt() == 2)
    assert(stats.get("maxValues").get("id").asInt() == 3)

    readSpec(t)
    snapshotSpec(t)
  }

  test("commit_partitioned_add") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("region", StringType).add("value", IntegerType),
      partitionColumns = Seq("region"))
    insertOp(w, Seq(Map("id" -> 1, "region" -> "east", "value" -> 10)))
    // The rows carry the partition column; the engine partitions and sets partitionValues.
    commitOp(w, addFiles = Some(Seq(AddFileInput(
      rows = Seq(Map("id" -> 2, "region" -> "west", "value" -> 20))))))
    val t = endWrite(w)

    val v = latestVersion(t)
    val add = actionsAt(t, v, "add").head
    assert(add.get("partitionValues").get("region").asText() == "west")

    readSpec(t)
    readSpec(t, predicate = "region = 'west'", name = Some("read_west"))
    snapshotSpec(t)
  }

  test("commit_multi_file") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("name", StringType))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    commitOp(w, addFiles = Some(Seq(
      AddFileInput(rows = Seq(Map("id" -> 2, "name" -> "bob"))),
      AddFileInput(rows = Seq(Map("id" -> 3, "name" -> "cara"))))))
    val t = endWrite(w)

    val v = latestVersion(t)
    assert(actionsAt(t, v, "add").size == 2, s"expected two add actions at v$v")

    readSpec(t)
    snapshotSpec(t)
  }

  test("commit_with_schema_and_properties") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("name", StringType))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    // A single low-level commit that evolves the schema, sets a property, AND adds a file.
    commitOp(w,
      schema = Some(new StructType().add("id", IntegerType).add("name", StringType).add("extra", StringType)),
      tableProperties = Some(Map("delta.appendOnly" -> "true")),
      addFiles = Some(Seq(AddFileInput(rows = Seq(Map("id" -> 2, "name" -> "bob"))))))
    val t = endWrite(w)

    val v = latestVersion(t)
    val meta = actionsAt(t, v, "metaData").head
    assert(meta.get("schemaString").asText().contains("extra"),
      s"schema: ${meta.get("schemaString").asText()}")
    assert(meta.get("configuration").get("delta.appendOnly").asText() == "true")

    readSpec(t)
    snapshotSpec(t)
  }

  test("commit_remove_multi_file_add") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("name", StringType))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    // One low-level commit adds TWO files; removing by that ordinal must tombstone BOTH.
    val added = commitOp(w, addFiles = Some(Seq(
      AddFileInput(rows = Seq(Map("id" -> 2, "name" -> "bob"))),
      AddFileInput(rows = Seq(Map("id" -> 3, "name" -> "cara"))))))
    val t = endWrite(w)
    val addedPaths = actionsAt(t, added.value.toLong, "add").map(_.get("path").asText()).toSet
    assert(addedPaths.size == 2, s"expected two adds at v$added, got $addedPaths")
    commitOp(w, removeFiles = Some(Seq(added)))

    val v = latestVersion(t)
    val removed = actionsAt(t, v, "remove").map(_.get("path").asText()).toSet
    assert(removed == addedPaths, s"both files tombstoned: removed=$removed added=$addedPaths")

    readSpec(t) // only id=1 remains
    snapshotSpec(t)
  }

  test("commit_with_column_mapping") {
    // Low-level adds go through the engine write path, so column mapping is handled: the logical
    // rows are written under the table's physical names with engine-computed (physical-keyed) stats.
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("name", StringType),
      properties = Map("delta.columnMapping.mode" -> "name"))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    commitOp(w, addFiles = Some(Seq(AddFileInput(rows = Seq(Map("id" -> 2, "name" -> "bob"))))))
    val t = endWrite(w)

    val v = latestVersion(t)
    assert(actionsAt(t, v, "add").size == 1, s"expected one add action at v$v")

    readSpec(t)
    readSpec(t, predicate = "name = 'bob'", name = Some("read_bob"))
    snapshotSpec(t)
  }
}
