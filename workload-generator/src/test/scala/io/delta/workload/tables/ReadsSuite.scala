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
 * Consolidated read workloads.
 * Merged from: core_reads.scala, core_reads_extended.scala, core_reads_legacy.scala
 */
class ReadsSuite extends WorkloadTestSuite("reads") {

  // === Core Reads ===

  test("basic") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("partitioned") {
    sql("CREATE TABLE tbl (id BIGINT, part INT) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl SELECT id, CAST(id % 5 AS INT) FROM range(100)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "part = 0")
    readSpec(t, predicate = "part = 3")
    snapshotSpec(t)
  }

  test("empty_path") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val logDir = dir.resolve("_delta_log")
      if (java.nio.file.Files.exists(logDir)) {
        java.nio.file.Files.walk(logDir).sorted(java.util.Comparator.reverseOrder())
          .forEach(p => java.nio.file.Files.deleteIfExists(p))
      }
    }
    readSpec(t)
  }

  test("append") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("overwrite") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    sql("INSERT OVERWRITE tbl SELECT id FROM range(100, 106)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("multiple_types") {
    sql("""CREATE TABLE tbl (
      id INT, name STRING, score DOUBLE, active BOOLEAN,
      created DATE, updated TIMESTAMP
    ) USING delta""")
    sql("INSERT INTO tbl VALUES (1,'alice',95.5,true,DATE'2024-01-01',TIMESTAMP'2024-01-01 10:00:00')")
    sql("INSERT INTO tbl VALUES (2,'bob',82.3,false,DATE'2024-02-15',TIMESTAMP'2024-02-15 14:30:00')")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("predicate") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 21)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value > 5")
    snapshotSpec(t)
  }

  test("bad_version") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    val t = registerTable("tbl")
    readSpec(t, version = 99)
    snapshotSpec(t)
  }

  test("version_zero") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
    sql("INSERT INTO tbl SELECT id FROM range(11, 16)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    snapshotSpec(t)
  }

  test("after_delete") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    sql("DELETE FROM tbl WHERE value <= 3")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("after_update") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    sql("UPDATE tbl SET value = value + 100 WHERE value <= 5")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value > 100")
    snapshotSpec(t)
  }

  test("after_merge") {
    sql("CREATE TABLE target (id INT, val STRING) USING delta")
    sql("INSERT INTO target VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("CREATE TABLE src (id INT, val STRING) USING delta")
    sql("INSERT INTO src VALUES (2,'updated'),(4,'new')")
    sql("""MERGE INTO target t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET val = s.val
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("target")
    readSpec(t)
    snapshotSpec(t)
  }

  test("nulls") {
    sql("CREATE TABLE tbl (id INT, name STRING, score DOUBLE, active BOOLEAN) USING delta")
    sql("INSERT INTO tbl VALUES (1,'alice',95.5,true)")
    sql("INSERT INTO tbl VALUES (2,null,null,null)")
    sql("INSERT INTO tbl VALUES (null,'charlie',88.0,false)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "name IS NOT NULL")
    snapshotSpec(t)
  }

  test("empty_partition") {
    sql("CREATE TABLE tbl (id BIGINT, part INT) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl SELECT id, CAST(id % 3 AS INT) FROM range(50)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "part = 99")
    snapshotSpec(t)
  }

  test("nested_struct") {
    sql("""CREATE TABLE tbl (
      id INT, info STRUCT<name: STRING, age: INT, address: STRUCT<city: STRING, zip: STRING>>
    ) USING delta""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30,'address',named_struct('city','NYC','zip','10001')))")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25,'address',named_struct('city','LA','zip','90001')))")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("array") {
    sql("CREATE TABLE tbl (id INT, tags ARRAY<STRING>, scores ARRAY<INT>) USING delta")
    sql("INSERT INTO tbl VALUES (1,array('a','b','c'),array(10,20,30))")
    sql("INSERT INTO tbl VALUES (2,array('x'),array(99))")
    sql("INSERT INTO tbl VALUES (3,array(),array())")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("map") {
    sql("CREATE TABLE tbl (id INT, props MAP<STRING, STRING>) USING delta")
    sql("INSERT INTO tbl VALUES (1,map('color','red','size','large'))")
    sql("INSERT INTO tbl VALUES (2,map('color','blue'))")
    sql("INSERT INTO tbl VALUES (3,map())")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("large_schema") {
    val colDefs = (1 to 24).map(i => s"col_$i BIGINT").mkString(", ")
    sql(s"CREATE TABLE tbl (id BIGINT, $colDefs) USING delta")
    val colExprs = (1 to 24).map(i => s"id * $i AS col_$i").mkString(", ")
    sql(s"INSERT INTO tbl SELECT id, $colExprs FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("special_chars") {
    sql("CREATE TABLE tbl (id INT, category STRING) USING delta PARTITIONED BY (category)")
    sql("INSERT INTO tbl VALUES (1,'hello world'),(2,'foo=bar'),(3,'a/b')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "category = 'hello world'")
    snapshotSpec(t)
  }

  test("schema_evolution") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    sql("ALTER TABLE tbl ADD COLUMN name STRING")
    sql("INSERT INTO tbl VALUES (6,'alice'),(7,'bob')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "name IS NOT NULL")
    val N = 3L
    for (v <- 0L to N) snapshotSpec(t, version = v)
  }

  test("rename_column") {
    sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob')")
    sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    sql("INSERT INTO tbl VALUES (3,'charlie')")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("decimal") {
    sql("CREATE TABLE tbl (id INT, price DECIMAL(10,2), ratio DECIMAL(18,8)) USING delta")
    sql("INSERT INTO tbl VALUES (1,99.99,0.12345678),(2,1234.56,3.14159265),(3,0.01,0.00000001)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "price > 100")
    snapshotSpec(t)
  }

  test("projection") {
    sql("CREATE TABLE tbl (id INT, name STRING, score DOUBLE, category STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'alice',95.5,'A'),(2,'bob',82.3,'B'),(3,'charlie',91.0,'A')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Seq("id", "name"))
    readSpec(t, columns = Seq("score"))
    snapshotSpec(t)
  }

  test("binary") {
    sql("CREATE TABLE tbl (id INT, data BINARY) USING delta")
    sql("INSERT INTO tbl VALUES (1,X'48454C4C4F'),(2,X'574F524C44'),(3,X'')")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("negative_version") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    val t = registerTable("tbl")
    readSpec(t, version = -1)
    snapshotSpec(t)
  }

  test("after_merge_target") {
    sql("CREATE TABLE target (id INT, val STRING) USING delta")
    sql("INSERT INTO target VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    sql("CREATE TABLE src (id INT, val STRING) USING delta")
    sql("INSERT INTO src VALUES (2, 'updated'), (4, 'new')")
    sql("""MERGE INTO target t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET val = s.val
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("target")
    readSpec(t)
    snapshotSpec(t)
  }

  test("byte_boundaries") {
    sql("CREATE TABLE tbl (b BYTE) USING delta")
    sql("INSERT INTO tbl VALUES (-128), (0), (127)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("short_boundaries") {
    sql("CREATE TABLE tbl (s SHORT) USING delta")
    sql("INSERT INTO tbl VALUES (-32768), (0), (32767)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("date_boundaries") {
    sql("CREATE TABLE tbl (d DATE) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'0001-01-01'), (DATE'2024-06-15'), (DATE'9999-12-31')")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("timestamp_boundaries") {
    sql("CREATE TABLE tbl (ts TIMESTAMP) USING delta")
    sql("""INSERT INTO tbl VALUES
      (TIMESTAMP'1970-01-01 00:00:00'),
      (TIMESTAMP'2024-06-15 12:30:45.123456'),
      (TIMESTAMP'2262-04-11 23:47:16.854775')""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("decimal_max_precision") {
    sql("CREATE TABLE tbl (d DECIMAL(38,18)) USING delta")
    sql("""INSERT INTO tbl VALUES
      (12345678901234567890.123456789012345678),
      (-12345678901234567890.123456789012345678),
      (0.000000000000000001)""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("decimal_zero_scale") {
    sql("CREATE TABLE tbl (d DECIMAL(38,0)) USING delta")
    sql("""INSERT INTO tbl VALUES
      (99999999999999999999999999999999999999),
      (-99999999999999999999999999999999999999),
      (0)""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("float_nan") {
    sql("CREATE TABLE tbl (f FLOAT) USING delta")
    sql("INSERT INTO tbl VALUES (CAST('NaN' AS FLOAT)), (1.5), (NULL)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "f IS NOT NULL")
    snapshotSpec(t)
  }

  test("float_infinity") {
    sql("CREATE TABLE tbl (f FLOAT) USING delta")
    sql("INSERT INTO tbl VALUES (CAST('Infinity' AS FLOAT)), (CAST('-Infinity' AS FLOAT)), (0.0)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "f > 0")
    snapshotSpec(t)
  }

  test("double_nan") {
    sql("CREATE TABLE tbl (d DOUBLE) USING delta")
    sql("INSERT INTO tbl VALUES (CAST('NaN' AS DOUBLE)), (2.5), (NULL)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "d IS NOT NULL")
    snapshotSpec(t)
  }

  test("double_infinity") {
    sql("CREATE TABLE tbl (d DOUBLE) USING delta")
    sql("INSERT INTO tbl VALUES (CAST('Infinity' AS DOUBLE)), (CAST('-Infinity' AS DOUBLE)), (0.0)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "d > 0")
    snapshotSpec(t)
  }

  test("deeply_nested_struct") {
    sql("""CREATE TABLE tbl (
      top STRUCT<l1: STRUCT<l2: STRUCT<l3: STRUCT<value: INT>>>>
    ) USING delta""")
    sql("""INSERT INTO tbl VALUES (
      named_struct('l1', named_struct('l2', named_struct('l3', named_struct('value', 42)))))""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("struct_all_null") {
    sql("CREATE TABLE tbl (s STRUCT<a: INT, b: STRING, c: DOUBLE>) USING delta")
    sql("INSERT INTO tbl VALUES (named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS STRING), 'c', CAST(NULL AS DOUBLE)))")
    sql("INSERT INTO tbl VALUES (named_struct('a', 1, 'b', 'hello', 'c', 3.14))")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("array_of_arrays") {
    sql("CREATE TABLE tbl (a ARRAY<ARRAY<INT>>) USING delta")
    sql("INSERT INTO tbl VALUES (array(array(1,2), array(3,4)))")
    sql("INSERT INTO tbl VALUES (array(array(), array(5)))")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("map_complex_value") {
    sql("CREATE TABLE tbl (m MAP<STRING, STRUCT<x: INT, y: STRING>>) USING delta")
    sql("INSERT INTO tbl VALUES (map('key1', named_struct('x', 1, 'y', 'a'), 'key2', named_struct('x', 2, 'y', 'b')))")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("wide_schema") {
    val colDefs = (1 to 100).map(i => s"col_$i INT").mkString(", ")
    sql(s"CREATE TABLE tbl ($colDefs) USING delta")
    val colExprs = (1 to 100).map(i => s"$i").mkString(", ")
    sql(s"INSERT INTO tbl VALUES ($colExprs)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("empty_vs_null_string") {
    sql("CREATE TABLE tbl (id INT, s STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, ''), (2, NULL), (3, 'hello')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "s IS NOT NULL")
    snapshotSpec(t)
  }

  test("binary_readback") {
    sql("CREATE TABLE tbl (id INT, data BINARY) USING delta")
    sql("INSERT INTO tbl VALUES (1, X'DEADBEEF'), (2, X''), (3, NULL)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "data IS NOT NULL")
    snapshotSpec(t)
  }

  test("boolean_filter") {
    sql("CREATE TABLE tbl (id INT, flag BOOLEAN) USING delta")
    sql("INSERT INTO tbl VALUES (1, true), (2, false), (3, NULL)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "flag = true")
    snapshotSpec(t)
  }

  test("zero_matching_rows") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "id > 999")
    readSpec(t, predicate = "id = -1")
    snapshotSpec(t)
  }

  test("projection_reorder") {
    sql("CREATE TABLE tbl (a INT, b STRING, c DOUBLE) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'hello', 3.14), (2, 'world', 2.72)")
    val t = registerTable("tbl")
    readSpec(t, columns = Seq("c", "a"))
    readSpec(t, columns = Seq("b"))
    snapshotSpec(t)
  }

  test("multi_partition") {
    sql("""CREATE TABLE tbl (id INT, year INT, region STRING)
      USING delta PARTITIONED BY (year, region)""")
    sql("INSERT INTO tbl VALUES (1, 2024, 'us'), (2, 2024, 'eu'), (3, 2025, 'us')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "year = 2024")
    readSpec(t, predicate = "year = 2024 AND region = 'us'")
    snapshotSpec(t)
  }

  test("partition_null") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, NULL), (3, 'b')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "part IS NULL")
    snapshotSpec(t)
  }

  test("oss_basic") {
    sql("""CREATE TABLE tbl (id LONG, data STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // 20 rows: id 0..19, data='oss_test'
    sql("INSERT INTO tbl SELECT id, 'oss_test' FROM range(20)")
    val t = registerTable("tbl")
    readSpec(t, name = "readAll")
    snapshotSpec(t)
  }

  test("oss_partitioned") {
    sql("""CREATE TABLE tbl (id INT, part STRING, value INT) USING delta
      PARTITIONED BY (part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'a',30),(4,'b',40),(5,'c',50)")
    val t = registerTable("tbl")
    readSpec(t, name = "readAll")
    readSpec(t, predicate = "part = 'a'", name = "readPartA")
    snapshotSpec(t)
  }

  test("oss_predicate") {
    sql("""CREATE TABLE tbl (id LONG, category STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // 50 rows: id 0..24 -> 'low', id 25..49 -> 'high'
    sql("""INSERT INTO tbl
      SELECT id, CASE WHEN id < 25 THEN 'low' ELSE 'high' END FROM range(50)""")
    val t = registerTable("tbl")
    readSpec(t, name = "readAll")
    readSpec(t, predicate = "id >= 40", name = "readHighId")
    readSpec(t, predicate = "category = 'low'", name = "readLow")
    snapshotSpec(t)
  }

  test("table_path_special") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // 10 rows: id 0..9
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    readSpec(t, predicate = "id < 5", name = "read_filtered")
    snapshotSpec(t)
  }

}
