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

class PartitionValuesSuite extends WorkloadTestSuite("partition_values") {

  test("boolean") {
    sql("""CREATE TABLE tbl (id INT, flag BOOLEAN) USING delta
      PARTITIONED BY (flag) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, true), (2, false), (3, true), (4, false)")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "flag = true", name = "filter_true")
    readSpec(t, predicate = "flag = false", name = "filter_false")
    snapshotSpec(t)
  }

  test("byte") {
    sql("""CREATE TABLE tbl (id INT, b BYTE) USING delta
      PARTITIONED BY (b) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(-128 AS BYTE)), (2, CAST(0 AS BYTE)), (3, CAST(127 AS BYTE)), (4, CAST(1 AS BYTE))")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "b = CAST(-128 AS BYTE)", name = "filter_min")
    readSpec(t, predicate = "b = CAST(127 AS BYTE)", name = "filter_max")
    readSpec(t, predicate = "b > CAST(0 AS BYTE)", name = "filter_positive")
    snapshotSpec(t)
  }

  test("decimal") {
    sql("""CREATE TABLE tbl (id INT, amount DECIMAL(10,2)) USING delta
      PARTITIONED BY (amount) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 99.99), (2, -100.50), (3, 0.01), (4, 12345.67)")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "amount = 99.99", name = "filter_eq")
    readSpec(t, predicate = "amount > 0", name = "filter_positive")
    readSpec(t, predicate = "amount = -100.50", name = "filter_boundary")
    snapshotSpec(t)
  }

  test("double") {
    sql("""CREATE TABLE tbl (id INT, d DOUBLE) USING delta
      PARTITIONED BY (d) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 3.14159), (2, -2.71828), (3, 1000000.001), (4, 0.0)")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "d > 0", name = "filter_positive")
    readSpec(t, predicate = "d > 100", name = "filter_large")
    snapshotSpec(t)
  }

  test("empty_string") {
    sql("""CREATE TABLE tbl (id INT, tag STRING) USING delta
      PARTITIONED BY (tag) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, ''), (2, 'hello'), (3, CAST(NULL AS STRING)), (4, 'world')")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "tag = ''", name = "filter_empty_string")
    readSpec(t, predicate = "tag IS NULL", name = "filter_null")
    readSpec(t, predicate = "tag IS NOT NULL AND tag != ''", name = "filter_nonempty")
    snapshotSpec(t)
  }

  test("float") {
    sql("""CREATE TABLE tbl (id INT, f FLOAT) USING delta
      PARTITIONED BY (f) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(1.5 AS FLOAT)), (2, CAST(-3.14 AS FLOAT)), (3, CAST(0.0 AS FLOAT)), (4, CAST(99.9 AS FLOAT))")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "f = CAST(1.5 AS FLOAT)", name = "filter_eq")
    readSpec(t, predicate = "f > CAST(0.0 AS FLOAT)", name = "filter_positive")
    snapshotSpec(t)
  }

  test("multi_cols") {
    sql("""CREATE TABLE tbl (id INT, value STRING, p_str STRING, p_int INT, p_bool BOOLEAN) USING delta
      PARTITIONED BY (p_str, p_int, p_bool) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, 'a', 'cat_a', 10, true), (2, 'b', 'cat_a', 20, false),
      (3, 'c', 'cat_b', 10, true), (4, 'd', 'cat_b', 30, false),
      (5, 'e', 'cat_a', 10, false), (6, 'f', 'cat_c', 40, true)""")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "p_str = 'cat_a'", name = "filter_str")
    readSpec(t, predicate = "p_str = 'cat_a' AND p_int = 10", name = "filter_str_and_int")
    readSpec(t, predicate = "p_int >= 20 AND p_int <= 30", name = "filter_int_range")
    readSpec(t, predicate = "p_bool = true", name = "filter_bool_only")
    readSpec(t, predicate = "p_str = 'cat_b' AND p_int = 10 AND p_bool = true", name = "filter_all_three")
    snapshotSpec(t)
  }

  test("null_value") {
    sql("""CREATE TABLE tbl (id INT, category STRING) USING delta
      PARTITIONED BY (category) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'A'), (2, CAST(NULL AS STRING)), (3, 'B'), (4, CAST(NULL AS STRING))")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "category = 'A'", name = "filter_eq")
    readSpec(t, predicate = "category IS NULL", name = "filter_null")
    readSpec(t, predicate = "category IS NOT NULL", name = "filter_not_null")
    snapshotSpec(t)
  }

  test("short") {
    sql("""CREATE TABLE tbl (id INT, s SHORT) USING delta
      PARTITIONED BY (s) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(-32768 AS SHORT)), (2, CAST(0 AS SHORT)), (3, CAST(32767 AS SHORT)), (4, CAST(100 AS SHORT))")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "s = CAST(-32768 AS SHORT)", name = "filter_min")
    readSpec(t, predicate = "s = CAST(32767 AS SHORT)", name = "filter_max")
    readSpec(t, predicate = "s >= CAST(0 AS SHORT) AND s <= CAST(100 AS SHORT)", name = "filter_range")
    snapshotSpec(t)
  }

  test("special_chars") {
    sql("""CREATE TABLE tbl (id INT, label STRING) USING delta
      PARTITIONED BY (label) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, 'hello world'), (2, 'caf\u00e9'), (3, 'a/b=c&d'), (4, 'normal')""")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "label = 'hello world'", name = "filter_space")
    readSpec(t, predicate = "label = 'caf\u00e9'", name = "filter_unicode")
    readSpec(t, predicate = "label = 'a/b=c&d'", name = "filter_special")
    snapshotSpec(t)
  }

  test("timestamp_ntz") {
    sql("""CREATE TABLE tbl (id INT, ts_ntz TIMESTAMP_NTZ) USING delta
      PARTITIONED BY (ts_ntz) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'2024-01-01 00:00:00'), (2, TIMESTAMP_NTZ'2024-06-15 12:30:00'),
      (3, TIMESTAMP_NTZ'2024-12-31 23:59:59')""")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "ts_ntz = TIMESTAMP_NTZ'2024-06-15 12:30:00'", name = "filter_eq")
    readSpec(t, predicate = "ts_ntz >= TIMESTAMP_NTZ'2024-06-01 00:00:00'", name = "filter_range")
    snapshotSpec(t)
  }

  test("timestamp") {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP) USING delta
      PARTITIONED BY (ts) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP'2024-01-01 00:00:00'), (2, TIMESTAMP'2024-06-15 12:30:00.123456'),
      (3, TIMESTAMP'2024-12-31 23:59:59')""")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "ts = TIMESTAMP'2024-06-15 12:30:00.123456'", name = "filter_eq")
    readSpec(t, predicate = "ts >= TIMESTAMP'2024-06-01 00:00:00'", name = "filter_range")
    snapshotSpec(t)
  }

}
