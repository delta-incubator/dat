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

class RowTrackingSuite extends WorkloadTestSuite("row_tracking") {

  test("basic_read") {
    sql("""CREATE TABLE tbl (test_data LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(100)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    snapshotSpec(t)
  }

  test("all_null_materialized") {
    sql("""CREATE TABLE tbl (test_data LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(100)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    snapshotSpec(t)
  }

  test("no_null_materialized") {
    sql("""CREATE TABLE tbl (test_data LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(100)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    snapshotSpec(t)
  }

  test("mixed_materialized") {
    sql("""CREATE TABLE tbl (test_data LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(100)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    snapshotSpec(t)
  }

  test("conflicting_columns") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    snapshotSpec(t)
  }

  test("filter_read") {
    sql("""CREATE TABLE tbl (test_data LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(100)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 0, predicate = "test_data < 50")
    snapshotSpec(t)
  }

  test("column_projection") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'alice', 1.0),(2, 'bob', 2.0),(3, 'charlie', 3.0)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Seq("id", "value"))
    snapshotSpec(t)
  }

  test("read_base_row_id") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(20)")
    sql("INSERT INTO tbl SELECT id + 20 FROM range(10)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    readSpec(t, predicate = "id >= 15")
    snapshotSpec(t)
  }

  test("read_row_id_and_index") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id + 10 FROM range(10)")
    sql("UPDATE tbl SET id = id + 100 WHERE id < 3")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, predicate = "id >= 100")
    snapshotSpec(t)
  }

  test("across_schema_evolution") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("ALTER TABLE tbl ADD COLUMN (name STRING)")
    sql("INSERT INTO tbl VALUES (100, 'new')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    readSpec(t, predicate = "name IS NOT NULL")
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
    snapshotSpec(t, version = 2)
  }

  test("version_migration") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(20)")
    sql("INSERT INTO tbl SELECT id + 20 FROM range(10)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')")
    sql("INSERT INTO tbl SELECT id + 30 FROM range(10)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, predicate = "id >= 20")
    snapshotSpec(t)
  }

}
