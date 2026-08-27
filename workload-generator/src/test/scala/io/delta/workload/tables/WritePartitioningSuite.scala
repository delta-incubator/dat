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
 * Partitioning-focused inserts: multi-column partition keys, high-cardinality partitioning, a
 * string partition column, and NULL partition values. Each partition combination lands in its own
 * directory, so these also exercise partitionValues encoding on the resulting Add actions.
 */
class WritePartitioningSuite extends WorkloadTestSuite("write_partitioning") {

  test("multi_column_partition") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("year", IntegerType)
        .add("month", IntegerType).add("day", IntegerType).add("v", StringType),
      partitionColumns = Seq("year", "month", "day"))
    insertOp(w, Seq(
      Map("id" -> 1, "year" -> 2023, "month" -> 1, "day" -> 15, "v" -> "a"),
      Map("id" -> 2, "year" -> 2023, "month" -> 1, "day" -> 16, "v" -> "b"),
      Map("id" -> 3, "year" -> 2023, "month" -> 2, "day" -> 1, "v" -> "c")))
    insertOp(w, Seq(
      Map("id" -> 4, "year" -> 2024, "month" -> 12, "day" -> 31, "v" -> "d"),
      Map("id" -> 5, "year" -> 2023, "month" -> 1, "day" -> 15, "v" -> "e")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "year = 2023 AND month = 1", name = Some("read_jan_2023"))
    readSpec(t, predicate = "year = 2024", name = Some("read_2024"))
    snapshotSpec(t)
  }

  test("high_cardinality_partition") {
    // 120 distinct partition values written in a single insert.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("p", IntegerType).add("v", IntegerType),
      partitionColumns = Seq("p"))
    insertOp(w, (0 until 120).map(i => Map("id" -> i, "p" -> i, "v" -> (i * 10))))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "p = 0", name = Some("read_p0"))
    readSpec(t, predicate = "p = 119", name = Some("read_p119"))
    snapshotSpec(t)
  }

  test("string_partition_column") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("country", StringType).add("v", IntegerType),
      partitionColumns = Seq("country"))
    insertOp(w, Seq(
      Map("id" -> 1, "country" -> "US", "v" -> 10),
      Map("id" -> 2, "country" -> "DE", "v" -> 20),
      Map("id" -> 3, "country" -> "JP", "v" -> 30),
      Map("id" -> 4, "country" -> "US", "v" -> 40)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "country = 'US'", name = Some("read_us"))
    snapshotSpec(t)
  }

  test("null_partition_value") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("region", StringType).add("v", IntegerType),
      partitionColumns = Seq("region"))
    insertOp(w, Seq(
      Map("id" -> 1, "region" -> "east", "v" -> 1),
      Map("id" -> 2, "region" -> null, "v" -> 2),
      Map("id" -> 3, "region" -> null, "v" -> 3)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "region IS NULL", name = Some("read_null_region"))
    readSpec(t, predicate = "region IS NOT NULL", name = Some("read_named_region"))
    snapshotSpec(t)
  }

  test("append_into_existing_partition") {
    // Several appends target the same partition value, so the partition accumulates files across
    // commits.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("region", StringType).add("v", IntegerType),
      partitionColumns = Seq("region"))
    insertOp(w, Seq(Map("id" -> 1, "region" -> "east", "v" -> 10)))
    insertOp(w, Seq(Map("id" -> 2, "region" -> "east", "v" -> 20)))
    insertOp(w, Seq(Map("id" -> 3, "region" -> "east", "v" -> 30)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "region = 'east'", name = Some("read_east"))
    snapshotSpec(t)
  }

  test("append_creating_new_partition") {
    // A later append introduces a partition value not seen in earlier commits.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("region", StringType).add("v", IntegerType),
      partitionColumns = Seq("region"))
    insertOp(w, Seq(Map("id" -> 1, "region" -> "east", "v" -> 1)))
    insertOp(w, Seq(Map("id" -> 2, "region" -> "west", "v" -> 2)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "region = 'west'", name = Some("read_new_partition"))
    snapshotSpec(t)
  }

  test("int_partition_column") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("p", IntegerType).add("v", StringType),
      partitionColumns = Seq("p"))
    insertOp(w, Seq(
      Map("id" -> 1, "p" -> 10, "v" -> "a"),
      Map("id" -> 2, "p" -> 20, "v" -> "b"),
      Map("id" -> 3, "p" -> 10, "v" -> "c")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "p = 10", name = Some("read_p10"))
    snapshotSpec(t)
  }

  test("date_partition_column") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("d", DateType).add("v", IntegerType),
      partitionColumns = Seq("d"))
    insertOp(w, Seq(
      Map("id" -> 1, "d" -> "2021-01-01", "v" -> 1),
      Map("id" -> 2, "d" -> "2021-01-01", "v" -> 2),
      Map("id" -> 3, "d" -> "2022-12-31", "v" -> 3)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "d = DATE '2021-01-01'", name = Some("read_jan"))
    snapshotSpec(t)
  }

  test("boolean_partition_column") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("flag", BooleanType).add("v", IntegerType),
      partitionColumns = Seq("flag"))
    insertOp(w, Seq(
      Map("id" -> 1, "flag" -> true, "v" -> 1),
      Map("id" -> 2, "flag" -> false, "v" -> 2),
      Map("id" -> 3, "flag" -> true, "v" -> 3)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "flag = true", name = Some("read_true"))
    snapshotSpec(t)
  }

  test("timestamp_partition_column") {
    // Partitioning by TIMESTAMP. The declared wall-clock values are interpreted in the session time
    // zone on write and read; the round-trip proves that normalization is consistent end to end.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("ts", TimestampType).add("v", IntegerType),
      partitionColumns = Seq("ts"))
    insertOp(w, Seq(
      Map("id" -> 1, "ts" -> "2021-01-01 12:00:00", "v" -> 1),
      Map("id" -> 2, "ts" -> "2021-01-01 12:00:00", "v" -> 2),
      Map("id" -> 3, "ts" -> "2021-06-15 08:30:00", "v" -> 3)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "ts = TIMESTAMP '2021-01-01 12:00:00'", name = Some("read_jan_noon"))
    snapshotSpec(t)
  }

  test("timestamp_ntz_partition_column") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("ntz", TimestampNTZType).add("v", IntegerType),
      partitionColumns = Seq("ntz"))
    insertOp(w, Seq(
      Map("id" -> 1, "ntz" -> "2021-01-01 00:00:00", "v" -> 1),
      Map("id" -> 2, "ntz" -> "2021-01-01 00:00:00", "v" -> 2),
      Map("id" -> 3, "ntz" -> "2022-02-02 02:02:02", "v" -> 3)))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("microsecond_precision_timestamp_partition_values") {
    // Sub-second precision in the partition value must survive the partitionValues string encoding.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("ts", TimestampType).add("v", IntegerType),
      partitionColumns = Seq("ts"))
    insertOp(w, Seq(
      Map("id" -> 1, "ts" -> "2021-01-01 00:00:00.000001", "v" -> 1),
      Map("id" -> 2, "ts" -> "2021-01-01 00:00:00.123456", "v" -> 2),
      Map("id" -> 3, "ts" -> "2021-01-01 00:00:00.999999", "v" -> 3)))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("special_character_string_partition_value") {
    // Partition values containing slashes, spaces, and quotes must be escaped in the partition
    // directory path and preserved through the partitionValues encoding.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("label", StringType).add("v", IntegerType),
      partitionColumns = Seq("label"))
    insertOp(w, Seq(
      Map("id" -> 1, "label" -> "a/ b", "v" -> 1),
      Map("id" -> 2, "label" -> "quote's", "v" -> 2),
      Map("id" -> 3, "label" -> "eq=sign", "v" -> 3)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "label = 'a/ b'", name = Some("read_slash_space"))
    snapshotSpec(t)
  }

  test("mixed_type_multi_column_partition") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("part_id", IntegerType)
        .add("part_status", StringType).add("v", IntegerType),
      partitionColumns = Seq("part_id", "part_status"))
    insertOp(w, Seq(
      Map("id" -> 1, "part_id" -> 1, "part_status" -> "open", "v" -> 10),
      Map("id" -> 2, "part_id" -> 1, "part_status" -> "closed", "v" -> 20),
      Map("id" -> 3, "part_id" -> 2, "part_status" -> "open", "v" -> 30)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "part_id = 1 AND part_status = 'open'", name = Some("read_open_1"))
    snapshotSpec(t)
  }

  test("reorder_partition_and_data_columns") {
    // Partition columns are declared out of schema order and the row maps list keys in yet another
    // order; name-based resolution places every value correctly regardless.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("region", StringType)
        .add("year", IntegerType).add("v", IntegerType),
      partitionColumns = Seq("year", "region"))
    insertOp(w, Seq(
      Map("v" -> 1, "region" -> "east", "id" -> 1, "year" -> 2023),
      Map("year" -> 2024, "id" -> 2, "v" -> 2, "region" -> "west")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "year = 2023 AND region = 'east'", name = Some("read_2023_east"))
    snapshotSpec(t)
  }
}
