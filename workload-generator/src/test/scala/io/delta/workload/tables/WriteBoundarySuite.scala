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
 * Boundary and special-value inserts: type extremes (int/long min/max), the special doubles
 * (NaN, +/-Infinity, signed zero), date/timestamp range ends, string edges, and high-precision
 * decimals.
 */
class WriteBoundarySuite extends WorkloadTestSuite("write_boundary") {

  test("integer_and_long_extremes") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("c_int", IntegerType).add("c_long", LongType))
    insertOp(w, Seq(
      Map("id" -> 1, "c_int" -> Int.MinValue, "c_long" -> Long.MinValue),
      Map("id" -> 2, "c_int" -> Int.MaxValue, "c_long" -> Long.MaxValue),
      Map("id" -> 3, "c_int" -> 0, "c_long" -> 0L)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "c_int = -2147483648", name = Some("read_int_min"))
    readSpec(t, predicate = "c_long = 9223372036854775807", name = Some("read_long_max"))
    snapshotSpec(t)
  }

  test("double_special_values") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("d", DoubleType))
    insertOp(w, Seq(
      Map("id" -> 1, "d" -> Double.NaN),
      Map("id" -> 2, "d" -> Double.PositiveInfinity),
      Map("id" -> 3, "d" -> Double.NegativeInfinity),
      Map("id" -> 4, "d" -> 0.0),
      Map("id" -> 5, "d" -> -0.0)))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("date_and_timestamp_range_ends") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("d", DateType).add("ts", TimestampType))
    insertOp(w, Seq(
      Map("id" -> 1, "d" -> "0001-01-01", "ts" -> "0001-01-01 00:00:00"),
      Map("id" -> 2, "d" -> "9999-12-31", "ts" -> "9999-12-31 23:59:59")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "d = DATE '9999-12-31'", name = Some("read_max_date"))
    snapshotSpec(t)
  }

  test("empty_and_large_strings") {
    val large = "x" * 8192 // multi-KB single value
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("s", StringType))
    insertOp(w, Seq(
      Map("id" -> 1, "s" -> ""),
      Map("id" -> 2, "s" -> large),
      Map("id" -> 3, "s" -> "normal")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "s = ''", name = Some("read_empty"))
    snapshotSpec(t)
  }

  test("high_precision_decimals") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType)
        .add("d380", DecimalType(38, 0))
        .add("d3818", DecimalType(38, 18))
        .add("d54", DecimalType(5, 4)))
    insertOp(w, Seq(
      Map("id" -> 1,
        "d380" -> "99999999999999999999999999999999999999",
        "d3818" -> "12345678901234567890.123456789012345678",
        "d54" -> "9.9999"),
      Map("id" -> 2,
        "d380" -> "-99999999999999999999999999999999999999",
        "d3818" -> "-99999999999999999999.999999999999999999",
        "d54" -> "-9.9999"),
      Map("id" -> 3, "d380" -> "0", "d3818" -> "0.000000000000000001", "d54" -> "0.0001")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "d54 < 0", name = Some("read_negative_d54"))
    snapshotSpec(t)
  }

  test("decimals_with_nulls") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("amount", DecimalType(20, 6)))
    insertOp(w, Seq(
      Map("id" -> 1, "amount" -> "123456789012.345678"),
      Map("id" -> 2, "amount" -> null),
      Map("id" -> 3, "amount" -> "-0.000001")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "amount IS NULL", name = Some("read_null_amount"))
    readSpec(t, predicate = "amount IS NOT NULL", name = Some("read_non_null_amount"))
    snapshotSpec(t)
  }
}
