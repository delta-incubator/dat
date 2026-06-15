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
 * CRC (version-checksum) read workloads. Delta writes a `<version>.crc` sidecar on each commit;
 * each test builds a table with plain SQL and declares a `crcSpec` asserting the core aggregate
 * fields (tableSizeBytes, numFiles, protocol) plus any feature-gated fields the `.crc` carries
 * (deletion-vector counts, set transactions, inCommitTimestamp).
 */
class CrcSuite extends WorkloadTestSuite("crc") {

  test("crc_basic") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
    val t = registerTable("tbl")
    crcSpec(t, version = 1)
    snapshotSpec(t)
  }

  test("crc_partitioned") {
    sql("CREATE TABLE tbl (id INT, part INT) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1,0),(2,1),(3,0)")
    sql("INSERT INTO tbl VALUES (4,1)")
    val t = registerTable("tbl")
    crcSpec(t, version = 1)
  }

  test("crc_after_delete") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    sql("INSERT INTO tbl VALUES (4),(5)")
    sql("DELETE FROM tbl WHERE id = 2")
    sql("INSERT INTO tbl VALUES (6)")
    val t = registerTable("tbl")
    // v3 is the delete commit; the trailing insert (v4) makes it non-latest.
    crcSpec(t, version = 3)
  }

  test("crc_with_deletion_vectors") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3),(4),(5)")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    sql("INSERT INTO tbl VALUES (6)")
    val t = registerTable("tbl")
    // v2 is the deletion-vector commit; the trailing insert (v3) makes it non-latest.
    crcSpec(t, version = 2)
  }

  test("crc_multiple_versions") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    sql("INSERT INTO tbl VALUES (2)")
    sql("INSERT INTO tbl VALUES (3)")
    sql("INSERT INTO tbl VALUES (4)")
    val t = registerTable("tbl")
    // Both targets are non-latest (table ends at v4).
    crcSpec(t, version = 1, name = "crc_v1")
    crcSpec(t, version = 3, name = "crc_v3")
  }
}
