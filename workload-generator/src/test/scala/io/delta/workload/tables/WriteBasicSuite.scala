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

import io.delta.workload.WorkloadTestSuite

/** Basic write workloads producing a write spec (specs/&lt;name&gt;_write.json) for Delta writer testing. */
class WriteBasicSuite extends WorkloadTestSuite("write_basic") {

  test("create_and_read") {
    val w = createTableOp("tbl", schema = "id INT, name STRING, score DOUBLE")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "score" -> 95.5),
      Map("id" -> 2, "name" -> "bob", "score" -> 87.3),
      Map("id" -> 3, "name" -> "charlie", "score" -> 72.1)))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "score > 90.0", name = "read_high_score")
    snapshotSpec(t)
  }

  test("create_with_properties") {
    val w = createTableOp("tbl",
      schema = "id INT, value STRING, amount INT",
      properties = Map(
        "delta.enableDeletionVectors" -> "true",
        "delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> "alpha", "amount" -> 100),
      Map("id" -> 2, "value" -> "beta", "amount" -> 200),
      Map("id" -> 3, "value" -> "gamma", "amount" -> 300)))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    snapshotSpec(t)
  }

  test("insert_multiple") {
    val w = createTableOp("tbl", schema = "id INT, category STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "category" -> "a"),
      Map("id" -> 2, "category" -> "a")))
    insertOp(w, Seq(
      Map("id" -> 3, "category" -> "b"),
      Map("id" -> 4, "category" -> "b"),
      Map("id" -> 5, "category" -> "b")))
    insertOp(w, Seq(Map("id" -> 6, "category" -> "c")))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    readSpec(t, version = 1, name = "read_v1")
    readSpec(t, version = 2, name = "read_v2")
    readSpec(t, version = 3, name = "read_v3")
    readSpec(t, predicate = "category = 'b'", name = "read_category_b")
    snapshotSpec(t)
  }

  test("delete_basic") {
    val w = createTableOp("tbl",
      schema = "id INT, value STRING, amount INT",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> "keep", "amount" -> 10),
      Map("id" -> 2, "value" -> "remove", "amount" -> 20),
      Map("id" -> 3, "value" -> "keep", "amount" -> 30),
      Map("id" -> 4, "value" -> "remove", "amount" -> 40),
      Map("id" -> 5, "value" -> "keep", "amount" -> 50)))
    deleteOp(w, predicate = "value = 'remove'")
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_after_delete")
    readSpec(t, version = 1, name = "read_before_delete")
    readSpec(t, predicate = "amount > 25", name = "read_large_amount")
    snapshotSpec(t)
  }

  test("update_basic") {
    val w = createTableOp("tbl",
      schema = "id INT, status STRING, count INT",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 1, "status" -> "pending", "count" -> 0),
      Map("id" -> 2, "status" -> "pending", "count" -> 0),
      Map("id" -> 3, "status" -> "active", "count" -> 5),
      Map("id" -> 4, "status" -> "pending", "count" -> 0)))
    updateOp(w, predicate = "status = 'pending'",
      set = Map("status" -> "'active'", "count" -> "count + 1"))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_after_update")
    readSpec(t, version = 1, name = "read_before_update")
    readSpec(t, predicate = "status = 'active'", name = "read_active")
    snapshotSpec(t)
  }

  test("alter_add_column") {
    val w = createTableOp("tbl", schema = "id INT, name STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice"),
      Map("id" -> 2, "name" -> "bob")))
    addColumnsOp(w, "email STRING")
    insertOp(w, Seq(
      Map("id" -> 3, "name" -> "charlie", "email" -> "charlie@test.com"),
      Map("id" -> 4, "name" -> "diana", "email" -> "diana@test.com")))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    readSpec(t, version = 1, name = "read_before_alter")
    readSpec(t, predicate = "email IS NULL", name = "read_null_email")
    readSpec(t, predicate = "email IS NOT NULL", name = "read_with_email")
    snapshotSpec(t)
  }

  test("alter_set_properties") {
    val w = createTableOp("tbl", schema = "id INT, value INT")
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 100),
      Map("id" -> 2, "value" -> 200),
      Map("id" -> 3, "value" -> 300)))
    setPropertiesOp(w, Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 4, "value" -> 400),
      Map("id" -> 5, "value" -> 500)))
    updateOp(w, predicate = "id <= 2", set = Map("value" -> "value + 1000"))
    val t = registerWriteSpec(w)
    readSpec(t, version = 1, name = "read_initial_insert")
    readSpec(t, version = 3, name = "read_before_update")
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "value > 1000", name = "read_updated")
    snapshotSpec(t)
  }

  test("delete_all_rows") {
    val w = createTableOp("tbl",
      schema = "id INT, data STRING",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 1, "data" -> "first"),
      Map("id" -> 2, "data" -> "second"),
      Map("id" -> 3, "data" -> "third"),
      Map("id" -> 4, "data" -> "fourth")))
    deleteOp(w, predicate = "true")
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_after_delete_all")
    readSpec(t, version = 1, name = "read_before_delete_all")
    snapshotSpec(t)
  }

  test("partitioned_insert") {
    val w = createTableOp("tbl",
      schema = "id INT, region STRING, revenue INT",
      partitionColumns = Seq("region"))
    insertOp(w, Seq(
      Map("id" -> 1, "region" -> "east", "revenue" -> 100),
      Map("id" -> 2, "region" -> "east", "revenue" -> 150)))
    insertOp(w, Seq(
      Map("id" -> 3, "region" -> "west", "revenue" -> 200),
      Map("id" -> 4, "region" -> "west", "revenue" -> 250),
      Map("id" -> 5, "region" -> "west", "revenue" -> 300)))
    insertOp(w, Seq(Map("id" -> 6, "region" -> "north", "revenue" -> 400)))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "region = 'east'", name = "read_east")
    readSpec(t, predicate = "region = 'west'", name = "read_west")
    readSpec(t, predicate = "region = 'north'", name = "read_north")
    readSpec(t, predicate = "revenue >= 200", name = "read_high_revenue")
    snapshotSpec(t)
  }

  test("replace_table_as_select") {
    val w = createTableOp("tbl", schema = "id INT, name STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice"),
      Map("id" -> 2, "name" -> "bob")))
    // Replace-as-select: new schema + new data in a single commit.
    replaceTableOp(w,
      schema = "id INT, label STRING, score DOUBLE",
      rows = Seq(
        Map("id" -> 10, "label" -> "x", "score" -> 1.5),
        Map("id" -> 20, "label" -> "y", "score" -> 2.5)))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    readSpec(t, version = 1, name = "read_before_replace")
    readSpec(t, predicate = "score > 2.0", name = "read_high_score")
    snapshotSpec(t)
  }

  test("replace_table_schema_only") {
    val w = createTableOp("tbl", schema = "id INT, name STRING")
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    // Pure replace (no data) then insert under the new schema.
    replaceTableOp(w,
      schema = "a BIGINT, b STRING",
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(Map("a" -> 100L, "b" -> "hello")))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    snapshotSpec(t)
  }

  test("replace_table_as_select_partitioned") {
    val w = createTableOp("tbl", schema = "id INT, name STRING")
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    replaceTableOp(w,
      schema = "id INT, region STRING, revenue INT",
      partitionColumns = Seq("region"),
      rows = Seq(
        Map("id" -> 10, "region" -> "east", "revenue" -> 100),
        Map("id" -> 20, "region" -> "west", "revenue" -> 200),
        Map("id" -> 30, "region" -> "east", "revenue" -> 300)))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "region = 'east'", name = "read_east")
    readSpec(t, predicate = "revenue > 150", name = "read_high")
    snapshotSpec(t)
  }

  test("alter_rename_column") {
    val w = createTableOp("tbl", schema = "id INT, name STRING",
      properties = Map("delta.columnMapping.mode" -> "name"))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice"), Map("id" -> 2, "name" -> "bob")))
    renameColumnOp(w, "name", "full_name")
    insertOp(w, Seq(Map("id" -> 3, "full_name" -> "charlie")))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    readSpec(t, version = 1, name = "read_before_rename")
    readSpec(t, predicate = "full_name = 'alice'", name = "read_by_renamed")
    snapshotSpec(t)
  }

  test("alter_drop_column") {
    val w = createTableOp("tbl", schema = "id INT, name STRING, scratch STRING",
      properties = Map("delta.columnMapping.mode" -> "name"))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "a", "scratch" -> "x")))
    dropColumnsOp(w, Seq("scratch"))
    insertOp(w, Seq(Map("id" -> 2, "name" -> "b")))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    readSpec(t, version = 1, name = "read_before_drop")
    snapshotSpec(t)
  }

  test("alter_unset_properties") {
    val w = createTableOp("tbl", schema = "id INT, value INT",
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(Map("id" -> 1, "value" -> 10), Map("id" -> 2, "value" -> 20)))
    setPropertiesOp(w, Map("delta.enableDeletionVectors" -> "true"))
    unsetPropertiesOp(w, Seq("delta.enableChangeDataFeed"))
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_all")
    snapshotSpec(t)
  }

  test("delete_with_dvs") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING, score INT",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "score" -> 90),
      Map("id" -> 2, "name" -> "bob", "score" -> 75),
      Map("id" -> 3, "name" -> "charlie", "score" -> 88),
      Map("id" -> 4, "name" -> "diana", "score" -> 92),
      Map("id" -> 5, "name" -> "eve", "score" -> 60),
      Map("id" -> 6, "name" -> "frank", "score" -> 85)))
    deleteOp(w, predicate = "score < 80")
    val t = registerWriteSpec(w)
    readSpec(t, name = "read_after_delete")
    readSpec(t, version = 1, name = "read_before_delete")
    readSpec(t, predicate = "score >= 85", name = "read_high_score")
    snapshotSpec(t)
  }

}
