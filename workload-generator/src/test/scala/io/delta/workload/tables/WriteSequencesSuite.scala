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

import io.delta.workload.{AddDomainMetadata, AddFileInput, AppTxn, WorkloadTestSuite}

/**
 * Multi-operation write workloads: long op sequences, property churn, schema evolution chains,
 * and high-level/low-level mixes. Stresses the commit-index==version invariant and time-travel
 * across many versions.
 */
class WriteSequencesSuite extends WorkloadTestSuite("write_sequences") {

  test("insert_update_delete_chain") {
    val w = createTableOp("tbl", schema = "id INT, status STRING, n INT",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, (1 to 3).map(i => Map("id" -> i, "status" -> "new", "n" -> 0)))
    updateOp(w, predicate = "id <= 2", set = Map("status" -> "'active'", "n" -> "n + 1"))
    deleteOp(w, predicate = "id = 3")
    insertOp(w, Seq(Map("id" -> 4, "status" -> "new", "n" -> 5)))
    updateOp(w, predicate = "status = 'new'", set = Map("n" -> "99"))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    readSpec(t, version = 1, name = "read_v1")
    readSpec(t, version = 2, name = "read_after_update")
    readSpec(t, version = 3, name = "read_after_delete")
    readSpec(t, predicate = "status = 'active'", name = "read_active")
    readSpec(t, predicate = "n = 99", name = "read_n99")
    snapshotSpec(t)
  }

  test("property_churn") {
    val w = createTableOp("tbl", schema = "id INT, v INT")
    insertOp(w, Seq(Map("id" -> 1, "v" -> 1)))
    setPropertiesOp(w, Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(Map("id" -> 2, "v" -> 2)))
    unsetPropertiesOp(w, Seq("delta.enableChangeDataFeed"))
    setPropertiesOp(w, Map("delta.enableChangeDataFeed" -> "true",
      "delta.enableDeletionVectors" -> "true"))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    snapshotSpec(t)
  }

  test("schema_evolution_chain") {
    // Column mapping enables rename/drop; chain add -> rename -> drop with data between each.
    val w = createTableOp("tbl", schema = "id INT, a STRING",
      properties = Map("delta.columnMapping.mode" -> "name"))
    insertOp(w, Seq(Map("id" -> 1, "a" -> "x")))
    addColumnsOp(w, "b INT")
    insertOp(w, Seq(Map("id" -> 2, "a" -> "y", "b" -> 10)))
    renameColumnOp(w, "a", "label")
    insertOp(w, Seq(Map("id" -> 3, "label" -> "z", "b" -> 20)))
    dropColumnsOp(w, Seq("b"))
    insertOp(w, Seq(Map("id" -> 4, "label" -> "w")))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    readSpec(t, version = 1, name = "read_initial")
    readSpec(t, version = 3, name = "read_after_add")
    readSpec(t, predicate = "label = 'x'", name = "read_renamed")
    snapshotSpec(t)
  }


  test("domain_metadata_add_then_remove") {
    val w = createTableOp("tbl", schema = "id INT, name STRING",
      properties = Map("delta.feature.domainMetadata" -> "supported"))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    commitOp(w, addDomainMetadata = Some(Seq(
      AddDomainMetadata("delta.test", """{"v":1}"""))))
    commitOp(w, removeDomainMetadata = Some(Seq("delta.test")))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    snapshotSpec(t)
  }

  test("low_and_high_level_mix") {
    val w = createTableOp("tbl", schema = "id INT, name STRING")
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    val added = commitOp(w, addFiles = Some(Seq(AddFileInput(rows = Seq(Map("id" -> 2, "name" -> "bob"))))))
    insertOp(w, Seq(Map("id" -> 3, "name" -> "cara")))
    deleteOp(w, predicate = "id = 1")
    commitOp(w, removeFiles = Some(Seq(added)))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    readSpec(t, version = 2, name = "read_after_lowlevel_add")
    snapshotSpec(t)
  }

  test("commit_txn_and_files") {
    val w = createTableOp("tbl", schema = "id INT, name STRING")
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    // A single low-level commit carrying both a SetTransaction and an AddFile.
    commitOp(w,
      txn = Some(AppTxn("streaming-app", 42L)),
      addFiles = Some(Seq(AddFileInput(rows = Seq(Map("id" -> 2, "name" -> "bob"))))))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    snapshotSpec(t)
  }
}
