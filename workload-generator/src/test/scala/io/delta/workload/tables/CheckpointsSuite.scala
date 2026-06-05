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

class CheckpointsSuite extends WorkloadTestSuite("checkpoints") {

  private def checkpoint(name: String): Unit = forceCheckpoint(name)

  // Existing 5 workloads

  test("classic") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(1, 101)")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(101, 201)")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(201, 301)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id > 250")
    snapshotSpec(t)
  }

  test("multi_version") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(1, 51)")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(51, 101)")
    checkpoint("tbl")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(101, 151)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    snapshotSpec(t)
  }

  test("last_checkpoint") {
    sql("CREATE TABLE tbl (id INT, val STRING) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '5')")
    for (i <- 1 to 7) sql(s"INSERT INTO tbl VALUES ($i, 'v$i')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 5)
    snapshotSpec(t)
    snapshotSpec(t, version = 5)
  }

  test("schema_evolution") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(50)")
    checkpoint("tbl")
    sql("ALTER TABLE tbl ADD COLUMN name STRING")
    sql("INSERT INTO tbl SELECT id, 'test' FROM range(50, 100)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    val N = 3L
    for (v <- 0L to N) snapshotSpec(t, version = v)
  }

  test("partitioned") {
    sql("CREATE TABLE tbl (id LONG, part INT) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl SELECT id, CAST(id % 5 AS INT) FROM range(100)")
    sql("INSERT INTO tbl SELECT id, CAST(id % 5 AS INT) FROM range(100, 200)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "part = 0")
    readSpec(t, predicate = "part = 3")
    snapshotSpec(t)
  }

  test("classic_checkpoint") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
    checkpoint("tbl")
    sql("INSERT INTO tbl VALUES (6,'f')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    snapshotSpec(t)
  }

  test("empty_table") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("many_commits") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    for (i <- 1 to 20) sql(s"INSERT INTO tbl VALUES ($i)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id > 15")
    snapshotSpec(t)
  }

  test("multiple") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    checkpoint("tbl")
    sql("INSERT INTO tbl VALUES (4),(5)")
    checkpoint("tbl")
    sql("INSERT INTO tbl VALUES (6)")
    val t = registerTable("tbl")
    readSpec(t, name = "read_latest")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    snapshotSpec(t)
  }

  test("read_after_version_delete") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2)")
    sql("INSERT INTO tbl VALUES (3),(4)")
    checkpoint("tbl")
    sql("INSERT INTO tbl VALUES (5),(6)")
    // Delete the JSON commit after checkpoint to simulate truncation
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      val jsonFile = logDir.resolve("00000000000000000003.json")
      if (java.nio.file.Files.exists(jsonFile)) java.nio.file.Files.delete(jsonFile)
    }
    readSpec(t, name = "read_at_checkpoint")
    snapshotSpec(t)
  }

  test("checkpoint_only_table") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("INSERT INTO tbl VALUES (3,'c')")
    checkpoint("tbl")
    // Delete all JSON files, leaving only the checkpoint
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      val stream = java.nio.file.Files.list(logDir)
      try {
        val iter = scala.collection.JavaConverters.asScalaIteratorConverter(stream.iterator()).asScala
        iter.filter(_.toString.endsWith(".json")).foreach(java.nio.file.Files.delete)
      } finally { stream.close() }
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("v2_basic") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id > 3")
    snapshotSpec(t)
  }

  test("v2_json") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'x'),(2,'y')")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("v2_compat") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(100)")
    checkpoint("tbl")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(100, 200)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    snapshotSpec(t)
  }

  test("v2_compat_json") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(50)")
    checkpoint("tbl")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(50, 100)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    snapshotSpec(t)
  }

  test("v2_after_dml") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
    sql("DELETE FROM tbl WHERE id = 2")
    sql("UPDATE tbl SET name = 'updated' WHERE id = 3")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id > 2")
    snapshotSpec(t)
  }

  test("v2_all_actions_in_manifest") {
    // Small table — all actions fit in manifest
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("v2_all_actions_in_manifest_parquet") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("v2_multipart_sidecar") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.checkpointInterval' = '1',
        'delta.enableDeletionVectors' = 'true')""")
    for (i <- 0 to 6) sql(s"INSERT INTO tbl SELECT CAST(id AS INT) FROM range(${i*15}, ${(i+1)*15})")
    val t = registerTable("tbl")
    readSpec(t, name = "read_latest")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2, name = "read_v2_two_sidecars")
    readSpec(t, version = 4, name = "read_v4_four_sidecars")
    readSpec(t, version = 5, name = "read_v5_part_size_100")
    snapshotSpec(t)
  }

  test("v2_multipart_sidecar_json") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.checkpointInterval' = '1',
        'delta.enableDeletionVectors' = 'true')""")
    for (i <- 0 to 6) sql(s"INSERT INTO tbl SELECT CAST(id AS INT) FROM range(${i*10}, ${(i+1)*10})")
    val t = registerTable("tbl")
    readSpec(t, name = "read_latest")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2, name = "read_v2_two_sidecars")
    readSpec(t, version = 3)
    readSpec(t, version = 4, name = "read_v4_four_sidecars")
    readSpec(t, version = 5, name = "read_v5_part_size_100")
    readSpec(t, version = 6)
    snapshotSpec(t)
  }

  test("v2_with_dvs") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all_from_checkpoint")
    snapshotSpec(t)
  }

  test("v2_with_dvs_json") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(20)")
    sql("DELETE FROM tbl WHERE id IN (3, 7, 15)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all_from_checkpoint")
    snapshotSpec(t)
  }

  test("v2_with_column_mapping") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Seq("name"))
    snapshotSpec(t)
  }

  test("v2_with_row_tracking") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true',
        'delta.enableRowTracking' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d')")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("v2_with_struct_stats") {
    sql("""CREATE TABLE tbl (id INT, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 1.5),(2, 2.5),(3, 3.5)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("v2_with_type_widening") {
    sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableTypeWidening' = 'true',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 100),(2, 200)")
    sql("ALTER TABLE tbl CHANGE COLUMN value TYPE LONG")
    sql("INSERT INTO tbl VALUES (3, 3000000000)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    snapshotSpec(t)
  }

  test("after_100_commits") {
    sql("CREATE TABLE tbl (id INT) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '1000')")
    // Use batch inserts to create many commits efficiently
    for (i <- 0 until 105) sql(s"INSERT INTO tbl VALUES ($i)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id > 100")
    snapshotSpec(t)
  }

  test("struct_array_map") {
    sql("""CREATE TABLE tbl (
      id INT,
      info STRUCT<name: STRING, age: INT>,
      tags ARRAY<STRING>,
      props MAP<STRING, INT>
    ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1, named_struct('name','alice','age',30), array('a','b'), map('x',1)),
      (2, named_struct('name','bob','age',25), array('c'), map('y',2,'z',3))""")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Seq("info", "tags"))
    snapshotSpec(t)
  }

  test("v2_multiple_sidecars") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    // Many inserts to generate multiple sidecar files
    for (i <- 0 to 9) sql(s"INSERT INTO tbl SELECT CAST(id AS INT), CONCAT('val', CAST(id AS STRING)) FROM range(${i*20}, ${(i+1)*20})")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id > 150")
    snapshotSpec(t)
  }

  test("corrupt_last_checkpoint") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    sql("INSERT INTO tbl VALUES (4),(5)")
    checkpoint("tbl")
    // Corrupt _last_checkpoint
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      val lc = logDir.resolve("_last_checkpoint")
      java.nio.file.Files.write(lc, "{ invalid json garbage }}}".getBytes("UTF-8"))
      // Remove Hadoop checksum sidecar to avoid ChecksumException on OSS Spark
      java.nio.file.Files.deleteIfExists(logDir.resolve("._last_checkpoint.crc"))
    }
    readSpec(t)
    readSpec(t, predicate = "id > 3")
    snapshotSpec(t)
  }

  test("missing_checkpoint_file") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    sql("INSERT INTO tbl VALUES (4),(5)")
    checkpoint("tbl")
    // Delete the actual checkpoint parquet but leave _last_checkpoint
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      val stream = java.nio.file.Files.list(logDir)
      try {
        val iter = scala.collection.JavaConverters.asScalaIteratorConverter(stream.iterator()).asScala
        iter.filter(_.toString.contains(".checkpoint.")).foreach(java.nio.file.Files.delete)
      } finally { stream.close() }
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("wrong_version_hint") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2)")
    checkpoint("tbl")
    sql("INSERT INTO tbl VALUES (3),(4)")
    sql("INSERT INTO tbl VALUES (5),(6)")
    checkpoint("tbl")
    // Overwrite _last_checkpoint to point to older version
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      val lc = logDir.resolve("_last_checkpoint")
      // Point to version 1 instead of version 3
      java.nio.file.Files.write(lc,
        """{"version":1,"size":3}""".getBytes("UTF-8"))
      // Remove Hadoop checksum sidecar to avoid ChecksumException on OSS Spark
      java.nio.file.Files.deleteIfExists(logDir.resolve("._last_checkpoint.crc"))
    }
    readSpec(t)
    readSpec(t, predicate = "id > 4")
    snapshotSpec(t)
  }

  test("multipart") {
    sql("SET spark.databricks.delta.checkpoint.partSize=100")
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointInterval' = '1000')""")
    for (i <- 0 to 4) sql(s"INSERT INTO tbl SELECT CAST(id AS INT) FROM range(${i*50}, ${(i+1)*50})")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t, name = "read_latest")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    readSpec(t, version = 3)
    readSpec(t, version = 4)
    snapshotSpec(t)
  }

  test("multipart_10_parts") {
    sql("SET spark.databricks.delta.checkpoint.partSize=30")
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointInterval' = '1000')""")
    for (i <- 0 to 5) sql(s"INSERT INTO tbl SELECT CAST(id AS INT) FROM range(${i*50}, ${(i+1)*50})")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id > 200")
    snapshotSpec(t)
  }

  test("incomplete_multipart") {
    sql("SET spark.databricks.delta.checkpoint.partSize=50")
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointInterval' = '1000')""")
    for (i <- 0 to 3) sql(s"INSERT INTO tbl SELECT CAST(id AS INT) FROM range(${i*30}, ${(i+1)*30})")
    checkpoint("tbl")
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      val stream = java.nio.file.Files.list(logDir)
      try {
        val iter = scala.collection.JavaConverters.asScalaIteratorConverter(stream.iterator()).asScala
        val parts = iter.filter(p => p.toString.contains(".checkpoint.") && p.toString.contains(".parquet")).toSeq
        if (parts.nonEmpty) java.nio.file.Files.delete(parts.head)
      } finally { stream.close() }
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("err_missing_metadata") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    checkpoint("tbl")
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      import scala.collection.JavaConverters._
      // Remove every file under _delta_log so the log dir is empty. Earlier
      // tests in this suite may have switched checkpoint policy (classic /
      // multipart / V2), so we can't assume the checkpoint filename — just
      // wipe the directory.
      java.nio.file.Files.list(logDir).iterator().asScala
        .foreach(java.nio.file.Files.delete)
    }
    // The log dir is empty — Spark should report DELTA_TABLE_NOT_FOUND.
    snapshotSpec(t, expectError = "DELTA_TABLE_NOT_FOUND")
  }

  test("err_missing_protocol") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    checkpoint("tbl")
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(logDir).iterator().asScala
        .foreach(java.nio.file.Files.delete)
    }
    // Same shape as err_missing_metadata above — empty log → table-not-found.
    snapshotSpec(t, expectError = "DELTA_TABLE_NOT_FOUND")
  }

}
