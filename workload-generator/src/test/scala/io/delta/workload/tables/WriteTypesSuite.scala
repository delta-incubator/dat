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
 * Type-coverage write workloads: exercises the harness's row-value coercion and the schema round-trip across
 * every supported scalar type, nulls, and string edge cases. Each test captures then the
 * framework replays + re-validates, so a passing test proves the type survives capture->replay.
 */
class WriteTypesSuite extends WorkloadTestSuite("write_types") {

  test("scalar_types") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("c_long", LongType).add("c_short", ShortType)
        .add("c_byte", ByteType).add("c_float", FloatType).add("c_double", DoubleType)
        .add("c_bool", BooleanType).add("c_str", StringType))
    insertOp(w, Seq(
      Map("id" -> 1, "c_long" -> 10000000000L, "c_short" -> 100, "c_byte" -> 5,
        "c_float" -> 1.5f, "c_double" -> 2.5, "c_bool" -> true, "c_str" -> "a"),
      Map("id" -> 2, "c_long" -> 20000000000L, "c_short" -> 200, "c_byte" -> 7,
        "c_float" -> 3.5f, "c_double" -> 9.5, "c_bool" -> false, "c_str" -> "b")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "c_bool = true", name = Some("read_true"))
    readSpec(t, predicate = "c_double > 5.0", name = Some("read_big_double"))
    readSpec(t, predicate = "c_long > 15000000000", name = Some("read_big_long"))
    snapshotSpec(t)
  }

  test("date_and_timestamp") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("d", DateType).add("ts", TimestampType).add("ntz", TimestampNTZType))
    insertOp(w, Seq(
      Map("id" -> 1, "d" -> "2021-01-15", "ts" -> "2021-01-15 10:30:00",
        "ntz" -> "2021-06-01 08:00:00"),
      Map("id" -> 2, "d" -> "2022-07-04", "ts" -> "2022-07-04 23:59:59",
        "ntz" -> "2022-12-31 00:00:00")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "d > DATE '2021-06-01'", name = Some("read_after_date"))
    readSpec(t, predicate = "ts < TIMESTAMP '2022-01-01 00:00:00'", name = Some("read_before_ts"))
    snapshotSpec(t)
  }

  test("decimal_type") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("amount", DecimalType(10, 2)))
    insertOp(w, Seq(
      Map("id" -> 1, "amount" -> "123.45"),
      Map("id" -> 2, "amount" -> "99.99"),
      Map("id" -> 3, "amount" -> "1000.00")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "amount > 100.00", name = Some("read_over_100"))
    snapshotSpec(t)
  }

  test("binary_type") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("data", BinaryType))
    insertOp(w, Seq(
      Map("id" -> 1, "data" -> "aGVsbG8="),       // "hello"
      Map("id" -> 2, "data" -> "d29ybGQ=")))       // "world"
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("nullable_columns") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("name", StringType).add("score", IntegerType).add("d", DateType))
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "score" -> 90, "d" -> "2021-01-01"),
      Map("id" -> 2, "name" -> null, "score" -> null, "d" -> null),
      Map("id" -> 3, "name" -> "bob", "score" -> 75, "d" -> null)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "name IS NULL", name = Some("read_null_name"))
    readSpec(t, predicate = "name IS NOT NULL", name = Some("read_named"))
    readSpec(t, predicate = "d IS NULL", name = Some("read_null_date"))
    snapshotSpec(t)
  }

  test("string_edge_cases") {
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("s", StringType))
    insertOp(w, Seq(
      Map("id" -> 1, "s" -> "it's a \"quoted\" value"),
      Map("id" -> 2, "s" -> "unicode: café ☕ — ünïcödé"),
      Map("id" -> 3, "s" -> ""),
      Map("id" -> 4, "s" -> "line1\nline2\ttab")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "s = ''", name = Some("read_empty"))
    readSpec(t, predicate = "id = 1", name = Some("read_apostrophe"))
    snapshotSpec(t)
  }

  test("nested_type_data_is_unsupported") {
    // Nested column TYPES round-trip in the schema, but nested DATA via the rows API is not
    // supported — the harness's row-value coercion fails loud rather than silently writing a wrong value.
    val w = createTableOp("tbl", schema = new StructType().add("id", IntegerType).add("s", new StructType().add("a", IntegerType)))
    val ex = intercept[IllegalArgumentException] {
      insertOp(w, Seq(Map("id" -> 1, "s" -> Map("a" -> 1))))
    }
    assert(ex.getMessage.toLowerCase.contains("unsupported"), s"message: ${ex.getMessage}")
  }

  test("partitioned_by_typed_column") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("d", DateType).add("amount", DecimalType(8, 2)), partitionColumns = Seq("d"))
    insertOp(w, Seq(
      Map("id" -> 1, "d" -> "2021-01-01", "amount" -> "10.00"),
      Map("id" -> 2, "d" -> "2021-01-01", "amount" -> "20.00"),
      Map("id" -> 3, "d" -> "2021-02-01", "amount" -> "30.00")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "d = DATE '2021-01-01'", name = Some("read_jan"))
    snapshotSpec(t)
  }
}
