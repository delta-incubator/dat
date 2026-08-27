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
 * Change Data Feed workloads. Each test builds a CDF-enabled table across several
 * versions, then declares `cdfSpec`s over version ranges. The captured change rows
 * carry the `_change_type`, `_commit_version`, and `_commit_timestamp` columns.
 */
class CdcSuite extends WorkloadTestSuite("cdc") {

  test("inserts") {
    sql("""CREATE TABLE tbl (id LONG, name STRING) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob')")
    sql("INSERT INTO tbl VALUES (3,'charlie'),(4,'dave')")
    sql("INSERT INTO tbl VALUES (5,'eve'),(6,'frank')")
    val t = registerTable("tbl")
    readSpec(t)
    cdfSpec(t, startVersion = 0, endVersion = 3)
    cdfSpec(t, startVersion = 1, endVersion = 1)
    snapshotSpec(t)
  }

  test("updates") {
    sql("""CREATE TABLE tbl (id LONG, value INT) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,100),(2,200),(3,300)")
    sql("UPDATE tbl SET value = value * 2 WHERE id = 1")
    sql("UPDATE tbl SET value = value + 50 WHERE id IN (2, 3)")
    val t = registerTable("tbl")
    readSpec(t)
    cdfSpec(t, startVersion = 0, endVersion = 3)
    cdfSpec(t, startVersion = 1, endVersion = 3)
    snapshotSpec(t)
  }

  test("deletes") {
    sql("""CREATE TABLE tbl (id LONG, name STRING) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    sql("DELETE FROM tbl WHERE id = 3")
    sql("DELETE FROM tbl WHERE id IN (1, 5)")
    val t = registerTable("tbl")
    readSpec(t)
    cdfSpec(t, startVersion = 0, endVersion = 3)
    cdfSpec(t, startVersion = 1, endVersion = 3)
    snapshotSpec(t)
  }

  test("mixed_dml") {
    sql("""CREATE TABLE tbl (id LONG, value INT) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,100),(2,200),(3,300)")
    sql("UPDATE tbl SET value = 999 WHERE id = 2")
    sql("DELETE FROM tbl WHERE id = 1")
    sql("INSERT INTO tbl VALUES (4,400)")
    val t = registerTable("tbl")
    readSpec(t)
    cdfSpec(t, startVersion = 0, endVersion = 4)
    cdfSpec(t, startVersion = 1, endVersion = 2)
    snapshotSpec(t)
  }

  test("predicate_filtered") {
    sql("""CREATE TABLE tbl (id LONG, value INT) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,100),(2,200),(3,300),(4,400)")
    sql("UPDATE tbl SET value = value + 1 WHERE id <= 2")
    sql("DELETE FROM tbl WHERE id = 4")
    val t = registerTable("tbl")
    readSpec(t)
    cdfSpec(t, startVersion = 0, endVersion = 3)
    cdfSpec(t, startVersion = 0, endVersion = 3, predicate = "value > 200")
    cdfSpec(t, startVersion = 0, endVersion = 3, predicate = "_change_type = 'delete'")
    snapshotSpec(t)
  }

  test("open_ended_to_latest") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id + 10 FROM range(10)")
    sql("INSERT INTO tbl SELECT id + 20 FROM range(10)")
    val t = registerTable("tbl")
    readSpec(t)
    cdfSpec(t, startVersion = 1)
    cdfSpec(t, startVersion = 0, endVersion = 3)
    snapshotSpec(t)
  }

}
