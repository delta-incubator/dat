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

/**
 * Type widening workloads: numeric chains, float->double, decimal precision,
 * date->timestampNTZ, nested fields, arrays, maps, partitions, DVs,
 * column mapping, data skipping, projections, and null handling.
 */
class TypeWideningSuite extends WorkloadTestSuite("type_widening") {

  // Simple type widenings

  test("byte_to_int") {
    sql("""CREATE TABLE tbl (a BYTE) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    // v1: insert byte-range values
    sql("INSERT INTO tbl VALUES (CAST(1 AS BYTE)), (CAST(127 AS BYTE))")
    // v2: widen byte -> short
    sql("ALTER TABLE tbl ALTER COLUMN a TYPE SHORT")
    // v3: insert short-range values
    sql("INSERT INTO tbl VALUES (CAST(128 AS SHORT)), (CAST(32767 AS SHORT))")
    // v4: widen short -> int
    sql("ALTER TABLE tbl ALTER COLUMN a TYPE INT")
    // v5: insert int-range values
    sql("INSERT INTO tbl VALUES (32768), (2147483647)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "a > 127", name = Some("read_gt_max_byte"))
    readSpec(t, predicate = "a > 32767", name = Some("read_gt_max_short"))
    for (v <- 0L to 5L) snapshotSpec(t, version = v)
  }

  test("short_to_int") {
    sql("""CREATE TABLE tbl (id INT, value SHORT) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(100 AS SHORT)), (2, CAST(32767 AS SHORT))")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    sql("INSERT INTO tbl VALUES (3, 40000), (4, 100000)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value > 32767", name = Some("read_gt_max_short"))
    snapshotSpec(t)
  }

  test("short_to_long") {
    sql("""CREATE TABLE tbl (id INT, value SHORT) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(100 AS SHORT)), (2, CAST(32767 AS SHORT))")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    sql("INSERT INTO tbl VALUES (3, 40000), (4, 2147483647)")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    sql("INSERT INTO tbl VALUES (5, 3000000000L), (6, 9223372036854775807L)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value > 32767", name = Some("read_gt_max_short"))
    readSpec(t, predicate = "value > 2147483647", name = Some("read_gt_max_int"))
    for (v <- 0L to 5L) snapshotSpec(t, version = v)
  }

  test("int_to_long") {
    sql("""CREATE TABLE tbl (a INT) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1), (2147483647)")
    sql("ALTER TABLE tbl ALTER COLUMN a TYPE LONG")
    sql("INSERT INTO tbl VALUES (2147483648L), (9223372036854775807L)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "a > 2147483647", name = Some("read_gt_max_int"))
    readSpec(t, version = 1, name = Some("read_v1_before_widening"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("full_numeric_chain") {
    sql("""CREATE TABLE tbl (a BYTE) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (CAST(1 AS BYTE)), (CAST(100 AS BYTE))")
    sql("ALTER TABLE tbl ALTER COLUMN a TYPE SHORT")
    sql("INSERT INTO tbl VALUES (CAST(200 AS SHORT)), (CAST(30000 AS SHORT))")
    sql("ALTER TABLE tbl ALTER COLUMN a TYPE INT")
    sql("INSERT INTO tbl VALUES (40000), (2000000000)")
    sql("ALTER TABLE tbl ALTER COLUMN a TYPE LONG")
    sql("INSERT INTO tbl VALUES (3000000000L), (9000000000000000000L)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_v1_byte_only"))
    readSpec(t, version = 3, name = Some("read_v3_through_short"))
    readSpec(t, version = 5, name = Some("read_v5_through_int"))
    for (v <- 0L to 7L) snapshotSpec(t, version = v)
  }

  test("float_to_double") {
    sql("""CREATE TABLE tbl (value FLOAT) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (CAST(1.5 AS FLOAT)), (CAST(3.14 AS FLOAT))")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE DOUBLE")
    sql("INSERT INTO tbl VALUES (3.141592653589793), (1.7976931348623157E308)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value > 100.0", name = Some("read_high_precision"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("decimal_precision") {
    sql("""CREATE TABLE tbl (amount DECIMAL(5,2)) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (123.45), (999.99)")
    sql("ALTER TABLE tbl ALTER COLUMN amount TYPE DECIMAL(10,2)")
    sql("INSERT INTO tbl VALUES (12345678.90), (99999999.99)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "amount > 1000", name = Some("read_large_values"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("cross_physical_decimal") {
    sql("""CREATE TABLE tbl (amount DECIMAL(9,2)) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (123.45), (9999999.99)")
    sql("ALTER TABLE tbl ALTER COLUMN amount TYPE DECIMAL(18,2)")
    sql("INSERT INTO tbl VALUES (1234567890123.45), (9999999999999999.99)")
    sql("ALTER TABLE tbl ALTER COLUMN amount TYPE DECIMAL(28,3)")
    sql("INSERT INTO tbl VALUES (1234567890123456789012345.678)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_v1_int32_only"))
    readSpec(t, version = 3, name = Some("read_v3_through_int64"))
    for (v <- 0L to 5L) snapshotSpec(t, version = v)
  }

  test("date_to_timestamp_ntz") {
    sql("""CREATE TABLE tbl (a DATE) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true',
        'delta.feature.timestampNtz' = 'supported')""")
    sql("INSERT INTO tbl VALUES (DATE'2024-01-15'), (DATE'2024-06-30')")
    sql("ALTER TABLE tbl ALTER COLUMN a TYPE TIMESTAMP_NTZ")
    sql("INSERT INTO tbl VALUES (TIMESTAMP_NTZ'2024-12-31 23:59:59'), (TIMESTAMP_NTZ'2025-01-01 12:30:00')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_v1_before_widening"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("nested_field") {
    sql("""CREATE TABLE tbl (data STRUCT<id: INT, count: INT>) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (named_struct('id', 1, 'count', 100))")
    sql("INSERT INTO tbl VALUES (named_struct('id', 2, 'count', 2000000000))")
    sql("ALTER TABLE tbl ALTER COLUMN data.count TYPE LONG")
    sql("INSERT INTO tbl VALUES (named_struct('id', 3, 'count', 3000000000L))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "data.count > 2147483647", name = Some("read_large_count"))
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("array_element") {
    sql("""CREATE TABLE tbl (values ARRAY<INT>) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (array(1, 2, 3))")
    sql("INSERT INTO tbl VALUES (array(100, 200, 2147483647))")
    sql("ALTER TABLE tbl ALTER COLUMN values.element TYPE LONG")
    sql("INSERT INTO tbl VALUES (array(3000000000L, 9000000000000L))")
    val t = registerTable("tbl")
    readSpec(t)
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("map_key_value_widening") {
    sql("""CREATE TABLE tbl (
      s STRUCT<a: BYTE>,
      m MAP<BYTE, SHORT>,
      a ARRAY<BYTE>
    ) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (named_struct('a', CAST(1 AS BYTE)), map(CAST(1 AS BYTE), CAST(10 AS SHORT)), array(CAST(1 AS BYTE), CAST(2 AS BYTE)))")
    // Widen map key: byte -> int
    sql("ALTER TABLE tbl ALTER COLUMN m.key TYPE INT")
    // Widen map value: short -> int
    sql("ALTER TABLE tbl ALTER COLUMN m.value TYPE INT")
    // Widen array element: byte -> int
    sql("ALTER TABLE tbl ALTER COLUMN a.element TYPE INT")
    // Widen struct field: byte -> int
    sql("ALTER TABLE tbl ALTER COLUMN s.a TYPE INT")
    sql("INSERT INTO tbl VALUES (named_struct('a', 50000), map(50000, 100000), array(50000, 60000))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_v1_before_widening"))
    for (v <- 0L to 6L) snapshotSpec(t, version = v)
  }

  test("with_dv") {
    sql("""CREATE TABLE tbl (id INT, value SHORT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableTypeWidening' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT)), (3, CAST(30 AS SHORT)), (4, CAST(40 AS SHORT)), (5, CAST(50 AS SHORT))")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    sql("INSERT INTO tbl VALUES (6, 40000), (7, 50000)")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value > 32767", name = Some("read_wide_values"))
    snapshotSpec(t)
  }

  test("with_partition") {
    sql("""CREATE TABLE tbl (id INT, value SHORT, category STRING) USING delta
      PARTITIONED BY (category) TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT), 'A'), (2, CAST(20 AS SHORT), 'B'), (3, CAST(30 AS SHORT), 'A')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    sql("INSERT INTO tbl VALUES (4, 40000, 'A'), (5, 50000, 'B')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "category = 'A'", name = Some("read_partition_A"))
    readSpec(t, predicate = "value > 32767", name = Some("read_wide_values"))
    snapshotSpec(t)
  }

  test("with_column_mapping") {
    sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableTypeWidening' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, 100), (2, 2000000000)")
    // Rename column first
    sql("ALTER TABLE tbl RENAME COLUMN value TO score")
    // Then widen
    sql("ALTER TABLE tbl ALTER COLUMN score TYPE LONG")
    sql("INSERT INTO tbl VALUES (3, 3000000000L), (4, 9000000000000L)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "score > 2147483647", name = Some("filter_on_widened"))
    readSpec(t, columns = Some(Seq("id", "score")), name = Some("project_renamed_widened"))
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("colmap_rename") {
    sql("""CREATE TABLE tbl (id INT, val SHORT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableTypeWidening' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(32767 AS SHORT))")
    // Widen first, then rename
    sql("ALTER TABLE tbl ALTER COLUMN val TYPE INT")
    sql("ALTER TABLE tbl RENAME COLUMN val TO amount")
    sql("INSERT INTO tbl VALUES (3, 40000), (4, 100000)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "amount > 32767", name = Some("filter_wide"))
    readSpec(t, columns = Some(Seq("id", "amount")), name = Some("project_renamed"))
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("change_tracking_across_widening") {
    sql("""CREATE TABLE tbl (id INT, amount SHORT) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableTypeWidening' = 'true',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(100 AS SHORT)), (2, CAST(200 AS SHORT))")
    sql("ALTER TABLE tbl ALTER COLUMN amount TYPE INT")
    sql("INSERT INTO tbl VALUES (3, 40000), (4, 50000)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_original"))
    snapshotSpec(t)
  }

  test("row_tracking_combo") {
    sql("""CREATE TABLE tbl (id INT, score SHORT) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true',
        'spark.delta.properties.defaults.enableRowTracking' = 'true',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT))")
    sql("ALTER TABLE tbl ALTER COLUMN score TYPE INT")
    sql("INSERT INTO tbl VALUES (3, 40000), (4, 50000)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "score > 32767", name = Some("read_wide_values"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("with_data_skipping") {
    sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 100), (2, 200)")
    sql("INSERT INTO tbl VALUES (3, 300), (4, 400)")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    sql("INSERT INTO tbl VALUES (5, 3000000000L), (6, 9000000000000L)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value > 2147483647", name = Some("read_large_values_only"))
    readSpec(t, predicate = "value > 150 AND value < 350", name = Some("read_with_predicate_on_widened"))
    snapshotSpec(t)
  }

  test("stats_after_change") {
    sql("""CREATE TABLE tbl (id INT, metric SHORT) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(100 AS SHORT))")
    sql("ALTER TABLE tbl ALTER COLUMN metric TYPE INT")
    sql("INSERT INTO tbl VALUES (3, 40000), (4, 50000)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "metric <= 100", name = Some("predicate_old_range"))
    readSpec(t, predicate = "metric > 32767", name = Some("predicate_new_range"))
    readSpec(t, predicate = "metric >= 100 AND metric <= 40000", name = Some("predicate_cross_range"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("project_widened") {
    sql("""CREATE TABLE tbl (id INT, value INT, label STRING) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 100, 'a'), (2, 200, 'b')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    sql("INSERT INTO tbl VALUES (3, 3000000000L, 'c'), (4, 4000000000L, 'd')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("value")), name = Some("project_widened_only"))
    readSpec(t, columns = Some(Seq("id", "value")), name = Some("project_widened_with_id"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("project_non_widened") {
    sql("""CREATE TABLE tbl (id INT, value INT, label STRING) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 100, 'a'), (2, 200, 'b')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    sql("INSERT INTO tbl VALUES (3, 3000000000L, 'c'), (4, 4000000000L, 'd')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "label")), name = Some("project_non_widened"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("null_handling") {
    sql("""CREATE TABLE tbl (id INT, value SHORT) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(NULL AS SHORT)), (3, CAST(30 AS SHORT)), (4, CAST(NULL AS SHORT))")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    sql("INSERT INTO tbl VALUES (5, 40000), (6, CAST(NULL AS INT))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value IS NOT NULL", name = Some("read_non_null"))
    readSpec(t, predicate = "value IS NULL", name = Some("read_nulls_only"))
    snapshotSpec(t)
  }

}
