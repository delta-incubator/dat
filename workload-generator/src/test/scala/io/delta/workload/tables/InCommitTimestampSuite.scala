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

class InCommitTimestampSuite extends WorkloadTestSuite("in_commit_timestamp") {

  test("basic") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    snapshotSpec(t)
  }

  test("create_or_replace") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("dml") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("UPDATE tbl SET id = id + 100 WHERE id < 5")
    sql("DELETE FROM tbl WHERE id >= 105")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    snapshotSpec(t)
  }

  test("enable_later") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'false', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')")
    sql("INSERT INTO tbl SELECT id + 10 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 3)
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
    snapshotSpec(t, version = 2)
    snapshotSpec(t, version = 3)
  }

  test("enabled_mid_lifecycle") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'false', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')")
    sql("INSERT INTO tbl SELECT id + 10 FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 15 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
    snapshotSpec(t, version = 2)
    snapshotSpec(t, version = 3)
    snapshotSpec(t, version = 4)
  }

  test("from_checkpoint") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES (
        'delta.enableInCommitTimestamps' = 'true',
        'delta.checkpointInterval' = '5',
        'delta.enableDeletionVectors' = 'true')""")
    for (i <- 0 to 5) sql(s"INSERT INTO tbl SELECT id + ${i * 5} FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("from_crc") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    snapshotSpec(t)
  }

  test("multiple_commits") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 10 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    readSpec(t, timestamp = t.getTimestampForVersion(0), name = "timestamp_v0")
    readSpec(t, timestamp = t.getTimestampForVersion(1), name = "timestamp_v1")
    readSpec(t, timestamp = t.getTimestampForVersion(2), name = "timestamp_v2")
    snapshotSpec(t)
  }

  test("time_travel") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id + 10 FROM range(10)")
    sql("INSERT INTO tbl SELECT id + 20 FROM range(10)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    readSpec(t, timestamp = t.getTimestampForVersion(0), name = "timestamp_v0")
    readSpec(t, timestamp = t.getTimestampForVersion(1), name = "timestamp_v1")
    readSpec(t, timestamp = t.getTimestampForVersion(2), name = "timestamp_v2")
    snapshotSpec(t)
  }

  test("with_checkpoint") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES (
        'delta.enableInCommitTimestamps' = 'true',
        'delta.checkpointInterval' = '3',
        'delta.enableDeletionVectors' = 'true')""")
    for (i <- 0 to 3) sql(s"INSERT INTO tbl SELECT id + ${i * 5} FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    readSpec(t, version = 2)
    snapshotSpec(t)
  }

}
