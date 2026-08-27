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

/**
 * Nested-value inserts: struct/array/map columns, their combinations, nulls at every level, and a
 * nested column alongside partitioning.
 */
class WriteNestedSuite extends WorkloadTestSuite("write_nested") {

  test("struct_column") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType)
        .add("s", new StructType().add("x", IntegerType).add("y", StringType)))
    insertOp(w, Seq(
      Map("id" -> 1, "s" -> Map("x" -> 10, "y" -> "a")),
      Map("id" -> 2, "s" -> Map("x" -> 20, "y" -> "b"))))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("array_column") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("nums", ArrayType(IntegerType)))
    insertOp(w, Seq(
      Map("id" -> 1, "nums" -> Seq(1, 2, 3)),
      Map("id" -> 2, "nums" -> Seq(4, 5))))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("map_column") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("m", MapType(StringType, IntegerType)))
    insertOp(w, Seq(
      Map("id" -> 1, "m" -> Map("a" -> 1, "b" -> 2)),
      Map("id" -> 2, "m" -> Map("c" -> 3))))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("array_of_struct") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType)
        .add("items", ArrayType(new StructType().add("id", IntegerType).add("name", StringType))))
    insertOp(w, Seq(
      Map("id" -> 1, "items" -> Seq(Map("id" -> 1, "name" -> "a"), Map("id" -> 2, "name" -> "b"))),
      Map("id" -> 2, "items" -> Seq(Map("id" -> 3, "name" -> "c")))))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("map_with_struct_value") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType)
        .add("m", MapType(StringType, new StructType().add("a", IntegerType).add("b", IntegerType))))
    insertOp(w, Seq(
      Map("id" -> 1, "m" -> Map("k1" -> Map("a" -> 1, "b" -> 2))),
      Map("id" -> 2, "m" -> Map("k2" -> Map("a" -> 3, "b" -> 4)))))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("deeply_nested_struct") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType)
        .add("s", new StructType().add("a", IntegerType)
          .add("inner", new StructType().add("b", IntegerType).add("c", StringType))))
    insertOp(w, Seq(
      Map("id" -> 1, "s" -> Map("a" -> 1, "inner" -> Map("b" -> 2, "c" -> "x"))),
      Map("id" -> 2, "s" -> Map("a" -> 3, "inner" -> Map("b" -> 4, "c" -> "y")))))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("null_struct") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType)
        .add("s", new StructType().add("x", IntegerType).add("y", StringType)))
    insertOp(w, Seq(
      Map("id" -> 1, "s" -> Map("x" -> 10, "y" -> "a")),
      Map("id" -> 2, "s" -> null)))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("null_nested_field") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType)
        .add("s", new StructType().add("x", IntegerType).add("y", StringType)))
    insertOp(w, Seq(
      Map("id" -> 1, "s" -> Map("x" -> 10, "y" -> null)),
      Map("id" -> 2, "s" -> Map("x" -> null, "y" -> "b"))))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("null_array_and_map") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType)
        .add("nums", ArrayType(IntegerType)).add("m", MapType(StringType, IntegerType)))
    insertOp(w, Seq(
      Map("id" -> 1, "nums" -> Seq(1, 2), "m" -> Map("a" -> 1)),
      Map("id" -> 2, "nums" -> null, "m" -> null)))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("nested_in_partitioned_table") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("region", StringType)
        .add("s", new StructType().add("x", IntegerType).add("y", StringType)),
      partitionColumns = Seq("region"))
    insertOp(w, Seq(
      Map("id" -> 1, "region" -> "east", "s" -> Map("x" -> 10, "y" -> "a")),
      Map("id" -> 2, "region" -> "west", "s" -> Map("x" -> 20, "y" -> "b"))))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "region = 'east'", name = Some("read_east"))
    snapshotSpec(t)
  }

  test("nested_struct_stats") {
    // A struct with leaf fields over multiple rows exercises per-leaf statistics generation.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType)
        .add("s", new StructType().add("n", IntegerType).add("label", StringType)))
    insertOp(w, Seq(
      Map("id" -> 1, "s" -> Map("n" -> 5, "label" -> "a")),
      Map("id" -> 2, "s" -> Map("n" -> 15, "label" -> "b")),
      Map("id" -> 3, "s" -> Map("n" -> 25, "label" -> "c"))))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }
}
