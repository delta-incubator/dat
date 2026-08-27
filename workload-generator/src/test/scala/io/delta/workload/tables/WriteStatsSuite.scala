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
 * Stats-generation inserts: each append drives the writer's per-file statistics (min/max/nullCount/
 * numRecords) down interesting paths -- all-null columns, string min/max prefix truncation, a
 * narrowed indexed-column count, and timestamp/decimal ranges. The point is that the corpus
 * exercises stats generation across these shapes.
 */
class WriteStatsSuite extends WorkloadTestSuite("write_stats") {

  test("partitioned_insert_stats_per_file") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("region", StringType).add("amount", IntegerType),
      partitionColumns = Seq("region"))
    insertOp(w, Seq(
      Map("id" -> 1, "region" -> "east", "amount" -> 10),
      Map("id" -> 2, "region" -> "east", "amount" -> 40),
      Map("id" -> 3, "region" -> "west", "amount" -> 25)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "amount > 20", name = Some("read_over_20"))
    snapshotSpec(t)
  }

  test("all_null_column") {
    // A column that is NULL in every row: nullCount equals numRecords and min/max are null.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("maybe", IntegerType))
    insertOp(w, Seq(
      Map("id" -> 1, "maybe" -> null),
      Map("id" -> 2, "maybe" -> null),
      Map("id" -> 3, "maybe" -> null)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "maybe IS NULL", name = Some("read_all_null"))
    snapshotSpec(t)
  }

  test("string_min_max_prefix_truncation") {
    // Strings longer than the default indexed prefix length share a common prefix and differ only
    // past it, so the min/max stats must be prefix-truncated by the writer.
    val prefix = "z" * 40
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("s", StringType))
    insertOp(w, Seq(
      Map("id" -> 1, "s" -> (prefix + "aaaa")),
      Map("id" -> 2, "s" -> (prefix + "mmmm")),
      Map("id" -> 3, "s" -> (prefix + "zzzz"))))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("utf8_boundary_string_min_max") {
    // A multi-byte UTF-8 character straddles the default 32-byte prefix boundary, so byte-length and
    // char-length disagree exactly where the writer truncates the min/max stat.
    val head = "a" * 31 // 31 ASCII bytes; the 32nd byte lands mid-character below.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("s", StringType))
    insertOp(w, Seq(
      Map("id" -> 1, "s" -> (head + "étail")),   // e-acute: 2 bytes
      Map("id" -> 2, "s" -> (head + "中tail")),   // CJK char: 3 bytes
      Map("id" -> 3, "s" -> (head + "😀")))) // emoji: 4 bytes
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("narrowed_indexed_columns") {
    // With dataSkippingNumIndexedCols narrowed to 3, only the first three columns get stats; the
    // remaining columns are present but unindexed.
    val w = createTableOp("tbl",
      schema = new StructType().add("c1", IntegerType).add("c2", IntegerType).add("c3", IntegerType)
        .add("c4", IntegerType).add("c5", StringType),
      properties = Map("delta.dataSkippingNumIndexedCols" -> "3"))
    insertOp(w, (1 to 5).map(i =>
      Map("c1" -> i, "c2" -> i * 2, "c3" -> i * 3, "c4" -> i * 4, "c5" -> s"v$i")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "c1 >= 3", name = Some("read_indexed"))
    readSpec(t, predicate = "c4 >= 12", name = Some("read_unindexed"))
    snapshotSpec(t)
  }

  test("timestamp_stats_across_precisions") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("ts", TimestampType))
    insertOp(w, Seq(
      Map("id" -> 1, "ts" -> "2021-01-01 00:00:00"),         // second precision
      Map("id" -> 2, "ts" -> "2021-01-01 00:00:00.123"),     // milli precision
      Map("id" -> 3, "ts" -> "2021-01-01 00:00:00.123456"))) // micro precision
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "ts > TIMESTAMP '2021-01-01 00:00:00.1'", name = Some("read_after_first"))
    snapshotSpec(t)
  }

  test("decimal_stats") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("amount", DecimalType(12, 4)))
    insertOp(w, Seq(
      Map("id" -> 1, "amount" -> "1.0001"),
      Map("id" -> 2, "amount" -> "99999999.9999"),
      Map("id" -> 3, "amount" -> "-500.5000")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "amount > 0", name = Some("read_positive"))
    snapshotSpec(t)
  }
}
