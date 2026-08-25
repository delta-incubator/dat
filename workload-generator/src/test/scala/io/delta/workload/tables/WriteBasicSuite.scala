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

import org.apache.spark.sql.types._

import io.delta.workload.WorkloadTestSuite

/** Basic write workloads producing a write spec (specs/&lt;name&gt;_write.json) for Delta writer testing. */
class WriteBasicSuite extends WorkloadTestSuite("write_basic") {

  test("create_and_read") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("name", StringType).add("score", DoubleType))
    insertOp(w, Seq(("alice", 95.5), ("bob", 87.3), ("charlie", 72.1)).zipWithIndex.map {
      case ((name, score), i) => Map("id" -> (i + 1), "name" -> name, "score" -> score) })
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "score > 90.0", name = Some("read_high_score"))
    snapshotSpec(t)
  }

  test("create_with_properties") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("value", StringType).add("amount", IntegerType),
      properties = Map(
        "delta.enableDeletionVectors" -> "true",
        "delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq("alpha", "beta", "gamma").zipWithIndex.map {
      case (v, i) => Map("id" -> (i + 1), "value" -> v, "amount" -> (i + 1) * 100) })
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("insert_multiple") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("category", StringType))
    insertOp(w, (1 to 2).map(i => Map("id" -> i, "category" -> "a")))
    insertOp(w, (3 to 5).map(i => Map("id" -> i, "category" -> "b")))
    insertOp(w, Seq(Map("id" -> 6, "category" -> "c")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_v1"))
    readSpec(t, version = 2, name = Some("read_v2"))
    readSpec(t, version = 3, name = Some("read_v3"))
    readSpec(t, predicate = "category = 'b'", name = Some("read_category_b"))
    snapshotSpec(t)
  }

  test("delete_basic") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("value", StringType).add("amount", IntegerType),
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, (1 to 5).map(i =>
      Map("id" -> i, "value" -> (if (i % 2 == 0) "remove" else "keep"), "amount" -> i * 10)))
    deleteOp(w, predicate = "value = 'remove'")
    val t = endWrite(w)
    readSpec(t, name = Some("read_after_delete"))
    readSpec(t, version = 1, name = Some("read_before_delete"))
    readSpec(t, predicate = "amount > 25", name = Some("read_large_amount"))
    snapshotSpec(t)
  }

  test("update_basic") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("status", StringType).add("count", IntegerType),
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, Seq(("pending", 0), ("pending", 0), ("active", 5), ("pending", 0)).zipWithIndex.map {
      case ((status, count), i) => Map("id" -> (i + 1), "status" -> status, "count" -> count) })
    updateOp(w, predicate = "status = 'pending'",
      set = Map("status" -> "'active'", "count" -> "count + 1"))
    val t = endWrite(w)
    readSpec(t, name = Some("read_after_update"))
    readSpec(t, version = 1, name = Some("read_before_update"))
    readSpec(t, predicate = "status = 'active'", name = Some("read_active"))
    snapshotSpec(t)
  }

  test("alter_add_column") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("name", StringType))
    insertOp(w, Seq("alice", "bob").zipWithIndex.map { case (n, i) => Map("id" -> (i + 1), "name" -> n) })
    addColumnsOp(w, new StructType().add("email", StringType))
    insertOp(w, Seq("charlie", "diana").zipWithIndex.map {
      case (n, i) => Map("id" -> (i + 3), "name" -> n, "email" -> s"$n@test.com") })
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_before_alter"))
    readSpec(t, predicate = "email IS NULL", name = Some("read_null_email"))
    readSpec(t, predicate = "email IS NOT NULL", name = Some("read_with_email"))
    snapshotSpec(t)
  }

  test("alter_set_properties") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("value", IntegerType))
    insertOp(w, (1 to 3).map(i => Map("id" -> i, "value" -> i * 100)))
    setPropertiesOp(w, Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, (4 to 5).map(i => Map("id" -> i, "value" -> i * 100)))
    updateOp(w, predicate = "id <= 2", set = Map("value" -> "value + 1000"))
    val t = endWrite(w)
    readSpec(t, version = 1, name = Some("read_initial_insert"))
    readSpec(t, version = 3, name = Some("read_before_update"))
    readSpec(t)
    readSpec(t, predicate = "value > 1000", name = Some("read_updated"))
    snapshotSpec(t)
  }

  test("delete_all_rows") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("data", StringType),
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, Seq("first", "second", "third", "fourth").zipWithIndex.map {
      case (d, i) => Map("id" -> (i + 1), "data" -> d) })
    deleteOp(w, predicate = "true")
    val t = endWrite(w)
    readSpec(t, name = Some("read_after_delete_all"))
    readSpec(t, version = 1, name = Some("read_before_delete_all"))
    snapshotSpec(t)
  }

  test("partitioned_insert") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("region", StringType).add("revenue", IntegerType),
      partitionColumns = Seq("region"))
    insertOp(w, Seq(100, 150).zipWithIndex.map {
      case (rev, i) => Map("id" -> (i + 1), "region" -> "east", "revenue" -> rev) })
    insertOp(w, Seq(200, 250, 300).zipWithIndex.map {
      case (rev, i) => Map("id" -> (i + 3), "region" -> "west", "revenue" -> rev) })
    insertOp(w, Seq(Map("id" -> 6, "region" -> "north", "revenue" -> 400)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "region = 'east'", name = Some("read_east"))
    readSpec(t, predicate = "region = 'west'", name = Some("read_west"))
    readSpec(t, predicate = "region = 'north'", name = Some("read_north"))
    readSpec(t, predicate = "revenue >= 200", name = Some("read_high_revenue"))
    snapshotSpec(t)
  }

  test("replace_table_as_select") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("name", StringType))
    insertOp(w, Seq("alice", "bob").zipWithIndex.map { case (n, i) => Map("id" -> (i + 1), "name" -> n) })
    // Replace-as-select: new schema + new data in a single commit.
    replaceTableOp(w,
      schema = new StructType().add("id", IntegerType).add("label", StringType).add("score", DoubleType),
      rows = Seq(("x", 1.5), ("y", 2.5)).zipWithIndex.map {
        case ((label, score), i) => Map("id" -> ((i + 1) * 10), "label" -> label, "score" -> score) })
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_before_replace"))
    readSpec(t, predicate = "score > 2.0", name = Some("read_high_score"))
    snapshotSpec(t)
  }

  test("replace_table_schema_only") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("name", StringType))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    // Pure replace (no data) then insert under the new schema.
    replaceTableOp(w,
      schema = new StructType().add("a", LongType).add("b", StringType),
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(Map("a" -> 100L, "b" -> "hello")))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("replace_table_as_select_partitioned") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("name", StringType))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    replaceTableOp(w,
      schema = new StructType().add("id", IntegerType).add("region", StringType).add("revenue", IntegerType),
      partitionColumns = Seq("region"),
      rows = Seq("east", "west", "east").zipWithIndex.map {
        case (region, i) => Map("id" -> ((i + 1) * 10), "region" -> region, "revenue" -> ((i + 1) * 100)) })
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "region = 'east'", name = Some("read_east"))
    readSpec(t, predicate = "revenue > 150", name = Some("read_high"))
    snapshotSpec(t)
  }

  test("alter_rename_column") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("name", StringType),
      properties = Map("delta.columnMapping.mode" -> "name"))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice"), Map("id" -> 2, "name" -> "bob")))
    renameColumnOp(w, "name", "full_name")
    insertOp(w, Seq(Map("id" -> 3, "full_name" -> "charlie")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_before_rename"))
    readSpec(t, predicate = "full_name = 'alice'", name = Some("read_by_renamed"))
    snapshotSpec(t)
  }

  test("alter_drop_column") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("name", StringType).add("scratch", StringType),
      properties = Map("delta.columnMapping.mode" -> "name"))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "a", "scratch" -> "x")))
    dropColumnsOp(w, Seq("scratch"))
    insertOp(w, Seq(Map("id" -> 2, "name" -> "b")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_before_drop"))
    snapshotSpec(t)
  }

  test("alter_unset_properties") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("value", IntegerType),
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(Map("id" -> 1, "value" -> 10), Map("id" -> 2, "value" -> 20)))
    setPropertiesOp(w, Map("delta.enableDeletionVectors" -> "true"))
    unsetPropertiesOp(w, Seq("delta.enableChangeDataFeed"))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("delete_with_dvs") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("name", StringType).add("score", IntegerType),
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, Seq(("alice", 90), ("bob", 75), ("charlie", 88), ("diana", 92), ("eve", 60),
      ("frank", 85)).zipWithIndex.map {
      case ((name, score), i) => Map("id" -> (i + 1), "name" -> name, "score" -> score) })
    deleteOp(w, predicate = "score < 80")
    val t = endWrite(w)
    readSpec(t, name = Some("read_after_delete"))
    readSpec(t, version = 1, name = Some("read_before_delete"))
    readSpec(t, predicate = "score >= 85", name = Some("read_high_score"))
    snapshotSpec(t)
  }

}
