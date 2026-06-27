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

class SchemaEvolutionSuite extends WorkloadTestSuite("schema_evolution") {

  test("add_column") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'before')")
    sql("ALTER TABLE tbl ADD COLUMN (new_col DOUBLE)")
    sql("INSERT INTO tbl VALUES (2, 'after', 3.14)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    readSpec(t, columns = Some(Seq("id", "new_col")))
    readSpec(t, predicate = "new_col IS NOT NULL")
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("add_nested_field") {
    sql("CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    sql("ALTER TABLE tbl ADD COLUMNS (info.email STRING)")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25,'email','bob@test.com'))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "info.email IS NULL")
    readSpec(t, predicate = "info.email IS NOT NULL")
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("rename") {
    sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'before')")
    sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    sql("INSERT INTO tbl VALUES (2, 'after')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    readSpec(t, columns = Some(Seq("id", "new_name")))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("drop_column") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'alice','v1'),(2,'bob','v2')")
    sql("ALTER TABLE tbl DROP COLUMN value")
    sql("INSERT INTO tbl VALUES (3,'charlie')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 2)
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("multiple_renames") {
    sql("""CREATE TABLE tbl (id INT, a STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'first')")
    sql("ALTER TABLE tbl RENAME COLUMN a TO b")
    sql("INSERT INTO tbl VALUES (2,'second')")
    sql("ALTER TABLE tbl RENAME COLUMN b TO c")
    sql("INSERT INTO tbl VALUES (3,'third')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "c")))
    for (v <- 0L to 5L) snapshotSpec(t, version = v)
  }

  test("predicate_on_added") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob')")
    sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    sql("INSERT INTO tbl VALUES (3,'charlie',95),(4,'diana',88)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "score > 90")
    readSpec(t, predicate = "score IS NULL")
    readSpec(t, predicate = "score IS NOT NULL")
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("add_col_pred_eq") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    sql("INSERT INTO tbl VALUES (4,'d',100),(5,'e',200),(6,'f',300)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "score = 200")
    readSpec(t, predicate = "score = 100 OR score IS NULL")
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("drop_col_pred") {
    sql("""CREATE TABLE tbl (id INT, name STRING, category STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'alice','A'),(2,'bob','B'),(3,'charlie','A')")
    sql("ALTER TABLE tbl DROP COLUMN category")
    sql("INSERT INTO tbl VALUES (4,'diana'),(5,'eve')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id > 3")
    readSpec(t, predicate = "name = 'alice'")
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("rename_pred") {
    sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie')")
    sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    sql("INSERT INTO tbl VALUES (4,'diana')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "new_name = 'alice'")
    readSpec(t, predicate = "new_name = 'diana'")
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("rename_partition") {
    sql("""CREATE TABLE tbl (id INT, category STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'A'),(2,'B'),(3,'A'),(4,'C')")
    sql("ALTER TABLE tbl RENAME COLUMN category TO cat")
    sql("INSERT INTO tbl VALUES (5,'A'),(6,'C')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "cat = 'A'")
    readSpec(t, predicate = "cat = 'C'")
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("drop_readd_same_name") {
    sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'hello'),(2,'world')")
    sql("ALTER TABLE tbl DROP COLUMN x")
    sql("ALTER TABLE tbl ADD COLUMN (x INT)")
    sql("INSERT INTO tbl VALUES (3,100),(4,200)")
    val t = registerTable("tbl")
    readSpec(t)
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("readd_pred") {
    sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'old1'),(2,'old2')")
    sql("ALTER TABLE tbl DROP COLUMN x")
    sql("ALTER TABLE tbl ADD COLUMN (x INT)")
    sql("INSERT INTO tbl VALUES (3,100),(4,200)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "x = 200")
    readSpec(t, predicate = "x IS NULL")
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("dv_pred_null") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie'),(4,'diana')")
    sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    sql("INSERT INTO tbl VALUES (5,'eve',90),(6,'frank',80)")
    sql("DELETE FROM tbl WHERE id IN (2, 5)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "score IS NULL")
    readSpec(t, predicate = "score IS NOT NULL")
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("rename_read_v1") {
    sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'before_rename')")
    sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    sql("INSERT INTO tbl VALUES (2,'after_rename')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("nested_field_pred") {
    sql("CREATE TABLE tbl (id INT, info STRUCT<name: STRING>) USING delta")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice'))")
    sql("ALTER TABLE tbl ADD COLUMNS (info.email STRING)")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','email','bob@x.com'))")
    sql("INSERT INTO tbl VALUES (3, named_struct('name','charlie','email','charlie@y.com'))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "info.email IS NULL")
    readSpec(t, predicate = "info.email IS NOT NULL")
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("proj_at_old_version") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'one'),(2,'two')")
    sql("ALTER TABLE tbl ADD COLUMNS (extra INT)")
    sql("INSERT INTO tbl VALUES (3,'three',300)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1, columns = Some(Seq("id", "value")))
    readSpec(t, columns = Some(Seq("id", "extra")))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("type_coercion_insert") {
    sql("CREATE TABLE tbl (id LONG, value LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1, 100)")
    // Insert int values into long columns (implicit coercion)
    sql("INSERT INTO tbl SELECT CAST(2 AS INT), CAST(200 AS INT)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value > 150")
    for (v <- 0L to 2L) snapshotSpec(t, version = v)
  }

  test("merge_with_evolution") {
    sql("""CREATE TABLE target (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'false')""")
    sql("INSERT INTO target VALUES (1,'alice'),(2,'bob')")
    sql("CREATE TABLE src (id INT, name STRING, score INT) USING delta")
    sql("INSERT INTO src VALUES (2,'bob_updated',95),(3,'charlie',88)")
    sql("""MERGE INTO target t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("target")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "score")))
    for (v <- 0L to 2L) snapshotSpec(t, version = v)
  }

  test("add_col_pred_null") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'alice')")
    sql("INSERT INTO tbl VALUES (2, 'bob')")
    sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    sql("INSERT INTO tbl VALUES (3, 'charlie', 95)")
    sql("INSERT INTO tbl VALUES (4, 'diana', 88)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "score > 90")
    readSpec(t, predicate = "score IS NOT NULL")
    readSpec(t, predicate = "score IS NULL")
    snapshotSpec(t)
  }

  test("add_col_read_v1") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'alice')")
    sql("INSERT INTO tbl VALUES (2, 'bob')")
    sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    sql("INSERT INTO tbl VALUES (3, 'charlie', 95)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    snapshotSpec(t)
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("add_column_with_default") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    sql("INSERT INTO tbl VALUES (2)")
    sql("ALTER TABLE tbl ADD COLUMNS (status STRING)")
    sql("INSERT INTO tbl VALUES (3, 'active')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "status")))
    snapshotSpec(t)
  }

  test("drop_col_read_v1") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'alice', 10)")
    sql("INSERT INTO tbl VALUES (2, 'bob', 20)")
    sql("ALTER TABLE tbl DROP COLUMN value")
    sql("INSERT INTO tbl VALUES (3, 'charlie')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    snapshotSpec(t)
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("nested_field_project") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25))")
    sql("ALTER TABLE tbl ADD COLUMNS (info.email STRING)")
    sql("INSERT INTO tbl VALUES (3, named_struct('name','charlie','age',35,'email','c@test.com'))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id")))
    readSpec(t, columns = Some(Seq("id", "info")))
    snapshotSpec(t)
  }

  test("pred_on_added_col") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'alice')")
    sql("INSERT INTO tbl VALUES (2, 'bob')")
    sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    sql("INSERT INTO tbl VALUES (3, 'charlie', 95)")
    sql("INSERT INTO tbl VALUES (4, 'diana', 88)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "score IS NOT NULL")
    snapshotSpec(t)
  }

  test("rename_chain") {
    sql("""CREATE TABLE tbl (id INT, a STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'first')")
    sql("ALTER TABLE tbl RENAME COLUMN a TO b")
    sql("INSERT INTO tbl VALUES (2, 'second')")
    sql("ALTER TABLE tbl RENAME COLUMN b TO c")
    sql("INSERT INTO tbl VALUES (3, 'third')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "c")))
    snapshotSpec(t)
  }

  test("rename_column") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'alice')")
    sql("INSERT INTO tbl VALUES (2, 'bob')")
    sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    sql("INSERT INTO tbl VALUES (3, 'charlie')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "full_name")))
    snapshotSpec(t)
  }

  test("rename_part_pred") {
    sql("""CREATE TABLE tbl (id INT, category STRING, value INT) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'A', 100)")
    sql("INSERT INTO tbl VALUES (2, 'B', 200)")
    sql("INSERT INTO tbl VALUES (3, 'A', 300)")
    sql("ALTER TABLE tbl RENAME COLUMN category TO cat")
    sql("INSERT INTO tbl VALUES (4, 'C', 400)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "cat = 'A'")
    readSpec(t, predicate = "cat = 'C'")
    snapshotSpec(t)
    for (v <- 0L to 5L) snapshotSpec(t, version = v)
  }

  test("rename_partition_column") {
    sql("""CREATE TABLE tbl (id INT, category STRING, value INT) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'A', 100)")
    sql("INSERT INTO tbl VALUES (2, 'B', 200)")
    sql("ALTER TABLE tbl RENAME COLUMN category TO group_name")
    sql("INSERT INTO tbl VALUES (3, 'C', 300)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "group_name", "value")))
    snapshotSpec(t)
  }

  test("add_nested_struct_field") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25))")
    sql("ALTER TABLE tbl ADD COLUMNS (info.email STRING)")
    sql("INSERT INTO tbl VALUES (3, named_struct('name','charlie','age',35,'email','c@test.com'))")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("drop_and_readd_same_name") {
    sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'hello')")
    sql("ALTER TABLE tbl DROP COLUMN x")
    sql("ALTER TABLE tbl ADD COLUMN (x INT)")
    sql("INSERT INTO tbl VALUES (2, 42)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("add_top_level_column") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.enableTypeWidening' = 'false')""")
    sql("INSERT INTO tbl VALUES (1, 'alice')")
    sql("INSERT INTO tbl VALUES (2, 'bob')")
    sql("ALTER TABLE tbl ADD COLUMNS (age INT)")
    sql("INSERT INTO tbl VALUES (3, 'charlie', 30)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "name", "age")))
    snapshotSpec(t)
  }

}
