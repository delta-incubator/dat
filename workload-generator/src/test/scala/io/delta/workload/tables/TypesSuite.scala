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
 * Types, edge cases, time travel, and error handling.
 */
class TypesSuite extends WorkloadTestSuite("types") {

  // === Basic Types ===

  test("all_primitive_types") {
    sql("""CREATE TABLE tbl (
      int_col INT, long_col BIGINT, double_col DOUBLE, float_col FLOAT,
      string_col STRING, bool_col BOOLEAN, binary_col BINARY,
      decimal_col DECIMAL(18,6), date_col DATE, ts_col TIMESTAMP
    ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1, 100000000000, 3.14, 2.718, 'hello', true, X'DEADBEEF', 123456.789012, DATE'2024-01-15', TIMESTAMP'2024-01-15 10:30:00'),
      (2, 200000000000, -1.5, 0.0, 'world', false, X'CAFEBABE', -99999.000001, DATE'2025-06-30', TIMESTAMP'2025-06-30 23:59:59'),
      (42, 0, 0.0, -1.0, '', true, X'00', 0.000000, DATE'1970-01-01', TIMESTAMP'1970-01-01 00:00:00')""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Seq("int_col", "string_col"))
    snapshotSpec(t)
  }

  test("nested_types") {
    sql("""CREATE TABLE tbl (
      id INT, info STRUCT<name: STRING, age: INT>,
      tags ARRAY<STRING>, props MAP<STRING, INT>
    ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1, named_struct('name','alice','age',30), array('a','b'), map('x',1,'y',2)),
      (2, named_struct('name','bob','age',25), array('c'), map('z',3))""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Seq("id", "info"))
    snapshotSpec(t)
  }

  test("null_values") {
    sql("CREATE TABLE tbl (int_col INT, string_col STRING, double_col DOUBLE) USING delta")
    sql("INSERT INTO tbl VALUES (NULL, NULL, NULL)")
    sql("INSERT INTO tbl VALUES (1, 'not null', 1.5)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "int_col IS NULL")
    readSpec(t, predicate = "int_col IS NOT NULL")
    snapshotSpec(t)
  }

  test("empty_table") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("single_row") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'only')")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("large_table") {
    sql("CREATE TABLE tbl (id BIGINT, value DOUBLE, category STRING) USING delta")
    sql("""INSERT INTO tbl
      SELECT id, rand() as value,
        CASE WHEN id % 5 = 0 THEN 'A' WHEN id % 5 = 1 THEN 'B'
             WHEN id % 5 = 2 THEN 'C' WHEN id % 5 = 3 THEN 'D' ELSE 'E' END
      FROM range(10000)""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "category = 'A'")
    readSpec(t, columns = Seq("id", "category"))
    snapshotSpec(t)
  }

  test("void_001_void_top_level") {
    sql("""CREATE TABLE tbl (id INT, void_col VOID) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, null FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("void_002_void_nested_struct") {
    sql("""CREATE TABLE tbl (
      id INT,
      info STRUCT<name: STRING, void_field: VOID>
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl SELECT id, named_struct('name', CAST(id AS STRING), 'void_field', null) FROM range(3)""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("void_005_void_schema_evolution") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    sql("ALTER TABLE tbl ADD COLUMN (void_col VOID)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("void_006_void_multiple_columns") {
    sql("""CREATE TABLE tbl (id INT, void_a VOID, void_b VOID) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, null, null FROM range(3)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("void_007_void_with_backticks") {
    sql("""CREATE TABLE tbl (id INT, `my.void` VOID) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, null FROM range(3)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("void_in_struct") {
    sql("""CREATE TABLE tbl (
      id INT,
      info STRUCT<label: STRING, void_val: VOID>
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl SELECT id, named_struct('label', CAST(id AS STRING), 'void_val', null) FROM range(3)""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("ntz_basic") {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'2024-01-15 10:30:00'),
      (2, TIMESTAMP_NTZ'2024-06-20 14:00:00'),
      (3, TIMESTAMP_NTZ'2024-12-31 23:59:59')""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "ts > TIMESTAMP_NTZ'2024-06-01 00:00:00'")
    snapshotSpec(t)
  }

  test("ntz_far_past") {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'1800-01-01 00:00:00'),
      (2, TIMESTAMP_NTZ'1899-12-31 23:59:59'),
      (3, TIMESTAMP_NTZ'1970-01-01 00:00:00')""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "ts < TIMESTAMP_NTZ'1900-01-01 00:00:00'")
    snapshotSpec(t)
  }

  test("ntz_mixed_tz_ntz") {
    sql("""CREATE TABLE tbl (id INT, ts_tz TIMESTAMP, ts_ntz TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP'2024-01-15 10:00:00', TIMESTAMP_NTZ'2024-01-15 10:00:00'),
      (2, TIMESTAMP'2024-06-20 14:00:00', TIMESTAMP_NTZ'2024-06-20 14:00:00'),
      (3, TIMESTAMP'2024-12-31 23:59:59', TIMESTAMP_NTZ'2024-12-31 23:59:59')""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "ts_ntz >= TIMESTAMP_NTZ'2024-06-01 00:00:00'")
    snapshotSpec(t)
  }

  test("ntz_partition") {
    sql("""CREATE TABLE tbl (id INT, value STRING, ts_part TIMESTAMP_NTZ) USING delta
      PARTITIONED BY (ts_part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, 'a', TIMESTAMP_NTZ'2024-01-01 00:00:00'),
      (2, 'b', TIMESTAMP_NTZ'2024-02-01 00:00:00'),
      (3, 'c', TIMESTAMP_NTZ'2024-01-01 00:00:00'),
      (4, 'd', TIMESTAMP_NTZ'2024-03-01 00:00:00')""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "ts_part = TIMESTAMP_NTZ'2024-01-01 00:00:00'")
    readSpec(t, predicate = "ts_part >= TIMESTAMP_NTZ'2024-02-01 00:00:00'")
    snapshotSpec(t)
  }

  test("ntz_stats") {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, TIMESTAMP_NTZ'2024-01-15 00:00:00'),(2, TIMESTAMP_NTZ'2024-03-20 00:00:00')")
    sql("INSERT INTO tbl VALUES (3, TIMESTAMP_NTZ'2024-06-15 00:00:00'),(4, TIMESTAMP_NTZ'2024-06-20 00:00:00')")
    sql("INSERT INTO tbl VALUES (5, TIMESTAMP_NTZ'2024-12-01 00:00:00'),(6, TIMESTAMP_NTZ'2024-12-31 00:00:00')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "ts >= TIMESTAMP_NTZ'2024-06-01 00:00:00' AND ts < TIMESTAMP_NTZ'2024-07-01 00:00:00'")
    readSpec(t, predicate = "ts >= TIMESTAMP_NTZ'2024-12-01 00:00:00'")
    snapshotSpec(t)
  }

  test("tntz_column_mapping") {
    sql("""CREATE TABLE tbl (id INT, event_time TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'2024-01-15 10:00:00'),
      (2, TIMESTAMP_NTZ'2024-06-20 14:00:00'),
      (3, TIMESTAMP_NTZ'2024-12-31 23:59:59')""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "event_time > TIMESTAMP_NTZ'2024-06-01 00:00:00'")
    snapshotSpec(t)
  }

  test("tntz_epoch") {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'1970-01-01 00:00:00'),
      (2, TIMESTAMP_NTZ'2024-01-01 00:00:00')""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "ts = TIMESTAMP_NTZ'1970-01-01 00:00:00'")
    snapshotSpec(t)
  }

  test("tntz_partition_filter") {
    sql("""CREATE TABLE tbl (id INT, value STRING, ts_part TIMESTAMP_NTZ) USING delta
      PARTITIONED BY (ts_part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, 'a', TIMESTAMP_NTZ'2024-01-01 00:00:00'),
      (2, 'b', TIMESTAMP_NTZ'2024-06-15 00:00:00'),
      (3, 'c', TIMESTAMP_NTZ'2024-12-25 00:00:00')""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "ts_part = TIMESTAMP_NTZ'2024-01-01 00:00:00'")
    readSpec(t, predicate = "ts_part > TIMESTAMP_NTZ'2024-06-01 00:00:00'")
    snapshotSpec(t)
  }

  test("tntz_time_travel") {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, TIMESTAMP_NTZ'2024-01-01 00:00:00')")
    sql("INSERT INTO tbl VALUES (2, TIMESTAMP_NTZ'2024-06-01 00:00:00')")
    sql("INSERT INTO tbl VALUES (3, TIMESTAMP_NTZ'2024-12-01 00:00:00')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    snapshotSpec(t)
  }

}
