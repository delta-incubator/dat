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

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, lit, map => sparkMap, when}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}

import io.delta.workload.{TableHandle, WorkloadTestSuite}
import io.delta.workload.deltaharness.{Format => HarnessFormat}
import io.delta.workload.engine.{RowComparison, SnapshotResolver}
import io.delta.workload.json.JsonUtil
import io.delta.workload.log.{Action, AddFile, CheckpointMetadata, CommitLog, DomainMetadata,
  Metadata, Protocol, SidecarFile, Txn}
import io.delta.workload.model._

class CheckpointsSuite extends WorkloadTestSuite("checkpoints")
    with ScopedSessionConf {

  private def checkpoint(name: String): Unit = forceCheckpoint(name)

  private def injectActions(t: TableHandle, version: Int, actions: Seq[Action]): Unit =
    injectCommitActions(t, version.toLong)(actions)

  // === Read/snapshot workloads over tables that have a checkpoint ===

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
    readSpec(t, name = Some("read_latest"))
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
    readSpec(t, name = Some("read_at_checkpoint"))
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

  test("prior_commits_removed") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'a')")
    sql("INSERT OVERWRITE tbl VALUES (2, 'b')")
    sql("INSERT OVERWRITE tbl VALUES (3, 'c')")
    checkpoint("tbl")
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      java.nio.file.Files.delete(logDir.resolve("00000000000000000000.json"))
      java.nio.file.Files.delete(logDir.resolve("00000000000000000001.json"))
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("struct_stats_only") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.checkpoint.writeStatsAsJson' = 'false')""")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    sql("INSERT INTO tbl VALUES (3, 'c')")
    checkpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("stats_disabled") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsStruct' = 'false',
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.dataSkippingNumIndexedCols' = '0')""")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    sql("INSERT OVERWRITE tbl VALUES (3, 'c')")
    checkpoint("tbl")
    val t = registerTable("tbl")
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
    readSpec(t, name = Some("read_latest"))
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2, name = Some("read_v2_two_sidecars"))
    readSpec(t, version = 4, name = Some("read_v4_four_sidecars"))
    readSpec(t, version = 5, name = Some("read_v5_part_size_100"))
    snapshotSpec(t)
  }

  test("v2_multipart_sidecar_json") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.checkpointInterval' = '1',
        'delta.enableDeletionVectors' = 'true')""")
    for (i <- 0 to 6) sql(s"INSERT INTO tbl SELECT CAST(id AS INT) FROM range(${i*10}, ${(i+1)*10})")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_latest"))
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2, name = Some("read_v2_two_sidecars"))
    readSpec(t, version = 3)
    readSpec(t, version = 4, name = Some("read_v4_four_sidecars"))
    readSpec(t, version = 5, name = Some("read_v5_part_size_100"))
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
    readSpec(t, name = Some("read_all_from_checkpoint"))
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
    readSpec(t, name = Some("read_all_from_checkpoint"))
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
    readSpec(t, columns = Some(Seq("name")))
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
    readSpec(t, columns = Some(Seq("info", "tags")))
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
    readSpec(t, name = Some("read_latest"))
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
    snapshotSpec(t, expectError = ErrorCode("DELTA_TABLE_NOT_FOUND"))
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
    snapshotSpec(t, expectError = ErrorCode("DELTA_TABLE_NOT_FOUND"))
  }

  // === checkpointSpec workloads: force a checkpoint and assert the file exists (contents are
  //     covered by the paired snapshot spec) ===

  test("cp_basic") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
    val t = registerTable("tbl")
    checkpointSpec(t, version = 2)
    readSpec(t)
    snapshotSpec(t)
  }

  test("cp_v2") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""") 
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
    val t = registerTable("tbl")
    checkpointSpec(t, version = 2)
    readSpec(t)
    snapshotSpec(t)
  }

  test("cp_partitioned") {
    sql("CREATE TABLE tbl (id INT, part INT) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1,0),(2,1),(3,0)")
    sql("INSERT INTO tbl VALUES (4,1),(5,0)")
    val t = registerTable("tbl")
    checkpointSpec(t, version = 2)
    readSpec(t)
  }

  test("cp_after_many_commits") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    for (i <- 1 to 6) sql(s"INSERT INTO tbl VALUES ($i)")
    val t = registerTable("tbl")
    checkpointSpec(t, version = 6)
    readSpec(t)
    snapshotSpec(t)
  }

  test("cp_with_set_transaction") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    sql("INSERT INTO tbl VALUES (2)")
    val t = registerTable("tbl")
    injectActions(t, 2, Seq(Txn("cp-app", version = 42, lastUpdated = Some(1))))
    checkpointSpec(t, version = 2)
    snapshotSpec(t)
    readSpec(t)
  }

  test("cp_with_domain_metadata") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
      'delta.feature.domainMetadata' = 'supported')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    sql("DELETE FROM tbl WHERE id = 2")
    val t = registerTable("tbl")
    injectActions(t, 2, Seq(DomainMetadata("cpDomain", configuration = "{\"k\":\"v\"}")))
    checkpointSpec(t, version = 2)
    snapshotSpec(t)
    readSpec(t)
  }

  // Physical checkpoint forms (JSON/Parquet top-level files, manifests, sidecars,
  // multi-part, structured stats, >64 KiB components, allFiles CRC). Session settings
  // stay active until the specs are captured.
  private def snapshotResult(protocol: Protocol, metadata: Metadata): SnapshotResult =
    SnapshotResult(
      ProtocolInfo(protocol.minReaderVersion, protocol.minWriterVersion,
        protocol.readerFeatures.map(_.toSeq.sorted),
        protocol.writerFeatures.map(_.toSeq.sorted)),
      MetadataInfo(metadata.id, metadata.name, metadata.description,
        HarnessFormat(metadata.format.provider, metadata.format.options),
        metadata.schemaString, metadata.partitionColumns, metadata.configuration,
        metadata.createdTime))

  private def assertReadRows(
      spec: ReadSpec,
      tablePath: Path,
      schema: StructType,
      rows: Seq[Row],
      name: String): Unit = {
    val actual = SnapshotResolver.applyFilters(
      SnapshotResolver.buildDeltaReader(spark, tablePath,
        spec.query.version, spec.query.timestamp), spec.query.predicate, spec.query.columns)
    assert(actual.schema == schema)
    RowComparison.assertRowsEqual(spark.createDataFrame(rows.asJava, schema), actual, name)
  }

  Seq(
    ("json_inline", "json", "v2", 1000, 0),
    ("json_manifest", "json", "v2", 1000, 1),
    ("parquet_single_part", "parquet", "classic", 1000, 0),
    ("parquet_manifest", "parquet", "v2", 1000, 1),
    ("parquet_sidecars", "parquet", "v2", 1, 2)
  ).foreach { case (name, format, policy, partSize, sidecarCount) =>
    test(name) {
      val settings = Map(
        "spark.databricks.delta.checkpointV2.topLevelFileFormat" -> format,
        "spark.databricks.delta.checkpoint.partSize" -> partSize.toString)
      settings.foreach { case (key, value) => setForTest(key, value) }
      sql(s"""CREATE TABLE tbl (id INT, value STRING) USING delta
        TBLPROPERTIES ('delta.checkpointPolicy' = '$policy',
          'delta.checkpointInterval' = '1000', 'delta.enableDeletionVectors' = 'false')""")
      sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
        (1, 'a'), (2, CAST(NULL AS STRING)), (2, CAST(NULL AS STRING)) AS v(id, value)""")
      sql("INSERT INTO tbl VALUES (3, 'c')")
      forceCheckpoint("tbl")
      sql("INSERT INTO tbl VALUES (4, 'd')")
      val t = registerTable("tbl")
      // Read the original commit actions before mutating the copied fixture.
      val actions = (0L to 2L).flatMap(v => CommitLog.read(t.sourcePath, v))
      val metadata = actions.collect { case m: Metadata => m }.last
      val protocol = actions.collect { case p: Protocol => p }.last
      val adds = actions.collect { case a: AddFile => a }
      assert(adds.size == 2)
      info("Original protocol: " + protocol.toJson)
      info("Original metadata: " + metadata.toJson)
      adds.foreach(a => info("Original add: " + a.toJson))
      val schema = new StructType().add("id", IntegerType).add("value", StringType)
      assert(DataType.fromJson(metadata.schemaString) == schema)
      val expectedSnapshot = snapshotResult(protocol, metadata)
      val checkpointRows = Seq(Row(1, "a"), Row(2, null), Row(2, null), Row(3, "c"))
      val latestRows = checkpointRows :+ Row(4, "d")
      var copiedTablePath: Path = null
      mutateTable(t) { tableDir =>
        copiedTablePath = tableDir
        val log = tableDir.resolve("_delta_log")
        val listing = Files.list(log)
        val files = try listing.iterator().asScala.toVector finally listing.close()
        val checkpoints = files.filter(_.getFileName.toString.contains(".checkpoint."))
        assert(checkpoints.size == 1)
        val checkpoint = checkpoints.head
        if (policy == "classic") {
          assert(checkpoint.getFileName.toString == "00000000000000000002.checkpoint.parquet")
          val frame = spark.read.parquet(checkpoint.toString)
          assert(frame.filter("add IS NOT NULL").count() == adds.size)
          assert(frame.filter("metaData IS NOT NULL").count() == 1)
          assert(frame.filter("protocol IS NOT NULL").count() == 1)
          assert(!Files.exists(log.resolve("_sidecars")))
        } else {
          assert(checkpoint.getFileName.toString.matches(
            s"00000000000000000002\\.checkpoint\\.[^.]+\\.$format"))
          if (name == "json_inline") {
            // V2 allows inline file actions; this writer normally emits sidecars.
            val checkpointAdds = adds.map(_.copy(dataChange = false))
            val inlineActions: Seq[Action] =
              Seq(protocol, metadata, CheckpointMetadata(2L)) ++ checkpointAdds
            Files.write(checkpoint, inlineActions.map(_.toJson).asJava, UTF_8)
            val sidecars = Files.list(log.resolve("_sidecars"))
            try sidecars.iterator().asScala.toVector.foreach(Files.delete)
            finally sidecars.close()
            Files.delete(log.resolve("_sidecars"))
            val content = Files.readAllLines(checkpoint, UTF_8).asScala.map(Action.parse).toVector
            assert(content.collect { case a: AddFile => a } == checkpointAdds)
            assert(content.collect { case c: CheckpointMetadata => c.version } == Seq(2L))
            assert(!content.exists(_.isInstanceOf[SidecarFile]))
          } else {
            val manifest = if (format == "json") {
              Files.readAllLines(checkpoint, UTF_8).asScala.map(Action.parse).toVector
            } else {
              spark.read.parquet(checkpoint.toString).toJSON.collect().toVector.map(Action.parse)
            }
            val references = manifest.collect { case s: SidecarFile => s }
            assert(references.size == sidecarCount)
            assert(!manifest.exists(_.isInstanceOf[AddFile]))
            assert(manifest.collect { case c: CheckpointMetadata => c.version } == Seq(2L))
            assert(manifest.collect { case m: Metadata => m } == Seq(metadata))
            assert(manifest.collect { case p: Protocol => p } == Seq(protocol))
            val sidecarPaths = references.map { ref =>
              val path = log.resolve("_sidecars").resolve(ref.path)
              assert(ref.path.endsWith(".parquet"))
              assert(Files.size(path) == ref.sizeInBytes)
              assert(Files.getLastModifiedTime(path).toMillis == ref.modificationTime)
              path
            }
            val sidecarListing = Files.list(log.resolve("_sidecars"))
            try assert(sidecarListing.iterator().asScala.toSet == sidecarPaths.toSet)
            finally sidecarListing.close()
            val content = spark.read.parquet(sidecarPaths.map(_.toString): _*)
            assert(content.count() == adds.size)
            assert(content.filter("add IS NOT NULL").count() == adds.size)
            assert(content.filter("remove IS NOT NULL").count() == 0)
            assert(content.select("add.path").collect().map(_.getString(0)).toSet ==
              adds.map(_.path).toSet)
          }
        }
        // Keep same-version JSON commit 2 for the OSS historical snapshot lookup.
        // This tests checkpoint plus commit 2, then version 3 tail replay.
        // Checkpoint-only historical snapshots remain a separate failing repro.
        files.filter(p => p.getFileName.toString.matches("[0-9]{20}\\.json"))
          .filter(_.getFileName.toString.take(20).toLong < 2L).foreach(Files.delete)
        files.filter(_.getFileName.toString.endsWith(".crc")).foreach(Files.delete)
        Files.deleteIfExists(log.resolve("_last_checkpoint"))
      }
      Seq(
        ("read_all", ReadQuery(), latestRows, ReadResult(5L, 3, 0L)),
        ("read_v2", ReadQuery(version = Some(2L)), checkpointRows, ReadResult(4L, 2, 0L)),
        ("read_filtered", ReadQuery(predicate = Some("id >= 3")),
          Seq(Row(3, "c"), Row(4, "d")), ReadResult(2L, 2, 1L))
      ).foreach { case (readName, query, rows, expected) =>
        readSpec(t, name = Some(readName), version = query.version.map(Long.box).orNull,
          predicate = query.predicate.orNull).assert { spec =>
          assert(spec.expectation == Succeeded(expected))
          assertReadRows(spec, copiedTablePath, schema, rows, readName)
        }
      }
      Seq(None, Some(2L)).foreach { version =>
        snapshotSpec(t, version = version.map(Long.box).orNull).assert { spec =>
          assert(spec.query.version.contains(version.getOrElse(3L)))
          assert(spec.expectation == Succeeded(expectedSnapshot))
        }
      }
    }
  }

  // stats: OSS exposes structured checkpoint stats, but no product metadata-clustering
  // control. These fixtures check shape and filtered correctness, never row groups READ.
  Seq(
    ("e2e_07_single_part", "classic", 1000, 1, 0),
    ("e2e_07_multipart", "classic", 2, 3, 0),
    ("e2e_07_sidecars", "v2", 2, 1, 3)
  ).foreach { case (name, policy, partSize, checkpointCount, sidecarCount) =>
    test(name) {
      val settings = Map(
        "spark.databricks.delta.checkpointV2.topLevelFileFormat" -> "parquet",
        "spark.databricks.delta.checkpoint.partSize" -> partSize.toString,
        "spark.databricks.delta.statsAsStructInCheckpoint.forcedDisabled" -> "false")
      settings.foreach { case (key, value) => setForTest(key, value) }
      sql(s"""CREATE TABLE tbl (id INT, value STRING) USING delta
        TBLPROPERTIES ('delta.checkpointPolicy' = '$policy',
          'delta.checkpointInterval' = '1000', 'delta.enableDeletionVectors' = 'false',
          'delta.checkpoint.writeStatsAsStruct' = 'true')""")
      val rowsByFile = (0 until 6).map { band =>
        val low = band * 10
        sql(s"""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
          ($low, 'band_$band'), (${low + 1}, CAST(NULL AS STRING)),
          (${low + 1}, CAST(NULL AS STRING)) AS v(id, value)""")
        Seq(Row(low, s"band_$band"), Row(low + 1, null), Row(low + 1, null))
      }
      forceCheckpoint("tbl")
      val t = registerTable("tbl")
      val actions = (0L to 6L).flatMap(v => CommitLog.read(t.sourcePath, v))
      val metadata = actions.collect { case m: Metadata => m }.last
      val protocol = actions.collect { case p: Protocol => p }.last
      val adds = actions.collect { case a: AddFile => a }
      assert(adds.size == 6 && adds.map(_.path).distinct.size == 6)
      info("stats original protocol: " + protocol.toJson)
      info("stats original metadata: " + metadata.toJson)
      adds.foreach(a => info("stats original add: " + a.toJson))
      val schema = new StructType().add("id", IntegerType).add("value", StringType)
      assert(DataType.fromJson(metadata.schemaString) == schema)
      assert(metadata.partitionColumns.isEmpty)
      assert(metadata.configuration("delta.checkpointPolicy") == policy)
      assert(metadata.configuration("delta.checkpoint.writeStatsAsStruct") == "true")
      val expectedSnapshot = snapshotResult(protocol, metadata)
      var copiedTablePath: Path = null
      mutateTable(t) { tableDir =>
        copiedTablePath = tableDir
        val log = tableDir.resolve("_delta_log")
        val listing = Files.list(log)
        val files = try listing.iterator().asScala.toVector finally listing.close()
        val checkpoints = files.filter(_.getFileName.toString.contains(".checkpoint."))
        assert(checkpoints.size == checkpointCount)
        if (name == "e2e_07_single_part") {
          assert(checkpoints.head.getFileName.toString ==
            "00000000000000000006.checkpoint.parquet")
        } else if (policy == "classic") {
          val expectedNames = (1 to 3).map { part =>
            f"00000000000000000006.checkpoint.$part%010d.0000000003.parquet"
          }.toSet
          assert(checkpoints.map(_.getFileName.toString).toSet == expectedNames)
        } else {
          assert(checkpoints.head.getFileName.toString.matches(
            "00000000000000000006\\.checkpoint\\.[^.]+\\.parquet"))
        }
        val top = spark.read.parquet(checkpoints.map(_.toString): _*)
        val topActions = top.toJSON.collect().toVector.map(Action.parse)
        assert(topActions.collect { case m: Metadata => m } == Seq(metadata))
        assert(topActions.collect { case p: Protocol => p } == Seq(protocol))
        val sidecarPaths = if (policy == "v2") {
          val refs = topActions.collect { case s: SidecarFile => s }
          assert(refs.size == sidecarCount && refs.map(_.path).distinct.size == sidecarCount)
          assert(topActions.collect { case c: CheckpointMetadata => c.version } == Seq(6L))
          assert(!topActions.exists(_.isInstanceOf[AddFile]))
          val paths = refs.map { ref =>
            val path = log.resolve("_sidecars").resolve(ref.path)
            assert(ref.path.endsWith(".parquet"))
            assert(Files.size(path) == ref.sizeInBytes)
            assert(Files.getLastModifiedTime(path).toMillis == ref.modificationTime)
            path
          }
          val listing = Files.list(log.resolve("_sidecars"))
          try assert(listing.iterator().asScala.toSet == paths.toSet)
          finally listing.close()
          paths
        } else {
          assert(!Files.exists(log.resolve("_sidecars")))
          Vector.empty[Path]
        }
        val fileActions = if (sidecarPaths.isEmpty) top else
          spark.read.parquet(sidecarPaths.map(_.toString): _*)
        assert(fileActions.filter("add IS NOT NULL").count() == 6)
        assert(fileActions.filter("remove IS NOT NULL").count() == 0)
        if (sidecarPaths.nonEmpty) assert(fileActions.count() == 6)
        assert(fileActions.select("add.path").collect().filterNot(_.isNullAt(0))
          .map(_.getString(0)).toSet == adds.map(_.path).toSet)
        val stats = fileActions.filter("add IS NOT NULL").select(
          "add.stats_parsed.numRecords", "add.stats_parsed.minValues.id",
          "add.stats_parsed.maxValues.id", "add.stats_parsed.nullCount.value").collect().toSeq
        assert(stats.toSet == (0 until 6).map { band =>
          Row(3L, band * 10, band * 10 + 1, 2L)
        }.toSet)
        // Footer row groups are physical shape evidence, not reader IO counters.
        val footerRows = (checkpoints ++ sidecarPaths).map { path =>
          val reader = ParquetFileReader.open(HadoopInputFile.fromPath(
            new HadoopPath(path.toUri), spark.sparkContext.hadoopConfiguration))
          try {
            val groups = reader.getFooter.getBlocks.asScala
            info(s"stats footer: ${path.getFileName} groups=${groups.size} " +
              s"rows=${groups.map(_.getRowCount).sum}")
            assert(groups.size <= 1)
            groups.map(_.getRowCount).sum
          } finally reader.close()
        }
        assert(footerRows.sum == (if (policy == "v2") 12L else 8L))
        // Retain same-version JSON 6 for OSS version lookup, with no post-checkpoint tail.
        // Missing commits 0..5 make the checkpoint required even though commit 6 exists.
        files.filter(p => p.getFileName.toString.matches("[0-9]{20}\\.json"))
          .filter(_.getFileName.toString.take(20).toLong < 6L).foreach(Files.delete)
        files.filter(_.getFileName.toString.endsWith(".crc")).foreach(Files.delete)
        Files.deleteIfExists(log.resolve("_last_checkpoint"))
      }
      Seq(
        ("read_all", None, rowsByFile.flatten, ReadResult(18L, 6, 0L)),
        ("read_high", Some("id >= 40"), rowsByFile.drop(4).flatten, ReadResult(6L, 2, 4L)),
        ("read_boundary", Some("id = 41"), Seq(Row(41, null), Row(41, null)),
          ReadResult(2L, 1, 5L)),
        ("read_empty", Some("id >= 100"), Seq.empty[Row], ReadResult(0L, 0, 6L))
      ).foreach { case (readName, predicate, rows, expected) =>
        readSpec(t, name = Some(readName), predicate = predicate.orNull).assert { spec =>
          assert(spec.expectation == Succeeded(expected))
          assertReadRows(spec, copiedTablePath, schema, rows, readName)
        }
      }
      snapshotSpec(t).assert { spec =>
        assert(spec.query.version.contains(6L))
        assert(spec.expectation == Succeeded(expectedSnapshot))
      }
    }
  }

  // size: 64 KiB is a bounded fixture boundary, not a product dispatch threshold.
  // These shapes assert portable read correctness, never distributed execution.
  Seq("manifest", "json", "sidecars", "single_part", "multipart", "crc_all_files")
    .foreach { shape =>
      test(s"e2e_43_$shape") {
        val isCrc = shape == "crc_all_files"
        val policy = if (Set("single_part", "multipart", "crc_all_files")(shape))
          "classic" else "v2"
        val settings = Map(
          "spark.databricks.delta.checkpointV2.topLevelFileFormat" -> "json",
          "spark.databricks.delta.checkpoint.partSize" ->
            (if (shape == "multipart") "2" else "1000"),
          "spark.databricks.delta.allFilesInCrc.enabled" -> "true",
          "spark.databricks.delta.allFilesInCrc.thresholdNumFiles" -> "100")
        settings.foreach { case (key, value) => setForTest(key, value) }
        sql(s"""CREATE TABLE tbl (id INT, value STRING) USING delta
          TBLPROPERTIES ('delta.checkpointPolicy' = '$policy',
            'delta.checkpointInterval' = '1000', 'delta.enableDeletionVectors' = 'false')""")
        val rowsByFile = (0 until 4).map { band =>
          val low = band * 10
          sql(s"""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
            ($low, 'band_$band'), (${low + 1}, CAST(NULL AS STRING)),
            (${low + 1}, CAST(NULL AS STRING)) AS v(id, value)""")
          Seq(Row(low, s"band_$band"), Row(low + 1, null), Row(low + 1, null))
        }
        forceCheckpoint("tbl")
        // A complete-file removal distinguishes active files from historical Adds in CRC.
        if (isCrc) sql("DELETE FROM tbl WHERE id < 10")
        sql("INSERT INTO tbl VALUES (99, 'tail')")
        val t = registerTable("tbl")
        val baseVersion = if (isCrc) 5L else 4L
        val latestVersion = baseVersion + 1L
        val actions = (0L to latestVersion).flatMap(v => CommitLog.read(t.sourcePath, v))
        val metadata = actions.collect { case m: Metadata => m }.last
        val protocol = actions.collect { case p: Protocol => p }.last
        val adds = actions.collect { case a: AddFile => a }
        assert(adds.size == 5 && adds.map(_.path).distinct.size == 5)
        val schema = new StructType().add("id", IntegerType).add("value", StringType)
        assert(DataType.fromJson(metadata.schemaString) == schema)
        assert(metadata.partitionColumns.isEmpty)
        assert(metadata.configuration("delta.checkpointPolicy") == policy)
        assert(metadata.configuration("delta.enableDeletionVectors") == "false")
        val expectedSnapshot = snapshotResult(protocol, metadata)
        val baseRows = (if (isCrc) rowsByFile.drop(1) else rowsByFile).flatten
        val latestRows = baseRows :+ Row(99, "tail")
        info(s"size $shape protocol: " + protocol.toJson)
        info(s"size $shape metadata: " + metadata.toJson)
        adds.foreach(a => info(s"size $shape original add: " + a.toJson))
        var copiedTablePath: Path = null
        mutateTable(t) { tableDir =>
          copiedTablePath = tableDir
          val log = tableDir.resolve("_delta_log")
          val listing = Files.list(log)
          val files = try listing.iterator().asScala.toVector finally listing.close()
          val checkpoints = files.filter(_.getFileName.toString.contains(".checkpoint."))
            .sortBy(_.getFileName.toString)
          assert(checkpoints.size == (if (shape == "multipart") 2 else 1))
          if (policy == "classic") {
            val names = if (shape == "multipart") (1 to 2).map { part =>
              f"00000000000000000004.checkpoint.$part%010d.0000000002.parquet"
            }.toSet else Set("00000000000000000004.checkpoint.parquet")
            assert(checkpoints.map(_.getFileName.toString).toSet == names)
          } else {
            assert(checkpoints.head.getFileName.toString.matches(
              "00000000000000000004\\.checkpoint\\.[^.]+\\.json"))
          }
          val manifest = if (policy == "v2") {
            Files.readAllLines(checkpoints.head, UTF_8).asScala.map(Action.parse).toVector
          } else Vector.empty[Action]
          val refs = manifest.collect { case s: SidecarFile => s }
          assert(refs.size == (if (policy == "v2") 1 else 0))
          val sidecars = refs.map(ref => log.resolve("_sidecars").resolve(ref.path))
          refs.zip(sidecars).foreach { case (ref, path) =>
            assert(Files.size(path) == ref.sizeInBytes)
            assert(Files.getLastModifiedTime(path).toMillis == ref.modificationTime)
          }
          if (policy == "v2") {
            assert(!manifest.exists(_.isInstanceOf[AddFile]))
            assert(manifest.collect { case c: CheckpointMetadata => c.version } == Seq(4L))
            assert(manifest.collect { case m: Metadata => m } == Seq(metadata))
            assert(manifest.collect { case p: Protocol => p } == Seq(protocol))
          } else assert(!Files.exists(log.resolve("_sidecars")))
          val boundary = 64L * 1024L
          // Deterministic incompressible padding keeps Parquet fixtures small but >64 KiB.
          val bytes = new Array[Byte](96 * 1024)
          new scala.util.Random(43L).nextBytes(bytes)
          val padding = java.util.Base64.getEncoder.encodeToString(bytes)
          val paddedFiles = shape match {
            case "sidecars" => sidecars
            case "single_part" | "multipart" => checkpoints
            case _ => Vector.empty[Path]
          }
          paddedFiles.foreach { path =>
            val frame = spark.read.parquet(path.toString)
            val padded = frame.withColumn("add", when(col("add").isNotNull,
              col("add").withField("tags", sparkMap(lit("e2e43.padding"), lit(padding))))
              .otherwise(col("add")))
            val temp = Files.createTempDirectory("e2e43-checkpoint-")
            try {
              padded.coalesce(1).write.mode("overwrite").parquet(temp.toString)
              val parts = Files.list(temp)
              val part = try parts.iterator().asScala
                .filter(_.getFileName.toString.endsWith(".parquet")).toVector
              finally parts.close()
              assert(part.size == 1)
              Files.move(part.head, path, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
            } finally org.apache.commons.io.FileUtils.deleteDirectory(temp.toFile)
          }
          if (shape == "manifest" || shape == "sidecars") {
            val updated = manifest.map {
              case c: CheckpointMetadata if shape == "manifest" =>
                c.copy(tags = Some(Map("e2e43.padding" -> padding)))
              case ref: SidecarFile =>
                val path = log.resolve("_sidecars").resolve(ref.path)
                ref.copy(sizeInBytes = Files.size(path),
                  modificationTime = Files.getLastModifiedTime(path).toMillis)
              case action => action
            }
            Files.write(checkpoints.head, updated.map(_.toJson).asJava, UTF_8)
          }
          if (shape == "json") {
            val lines = CommitLog.readRaw(tableDir, latestVersion).map { line =>
              val node = JsonUtil.mapper.readTree(line)
              if (node.has("commitInfo")) {
                val commitInfo = node.get("commitInfo")
                  .asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
                commitInfo.put("userMetadata", padding)
              }
              node.toString
            }
            CommitLog.writeRaw(tableDir, latestVersion, lines)
          }
          val fileActions = spark.read.parquet(
            (if (policy == "v2") sidecars else checkpoints).map(_.toString): _*)
          val checkpointAdds = fileActions.toJSON.collect().toVector.map(Action.parse)
            .collect { case a: AddFile => a }
          val expectedAdds = adds.take(4).map { add =>
            add.copy(dataChange = false, tags = if (paddedFiles.nonEmpty)
              Some(Map("e2e43.padding" -> padding)) else add.tags)
          }
          assert(checkpointAdds.sortBy(_.path) == expectedAdds.sortBy(_.path))
          if (policy == "classic") {
            val top = fileActions.toJSON.collect().toVector.map(Action.parse)
            assert(top.collect { case m: Metadata => m } == Seq(metadata))
            assert(top.collect { case p: Protocol => p } == Seq(protocol))
          }
          if (isCrc) {
            Seq(baseVersion, latestVersion).foreach { version =>
              val crcPath = log.resolve(f"$version%020d.crc")
              assert(Files.exists(crcPath))
              val crc = JsonUtil.mapper.readTree(Files.readString(crcPath, UTF_8))
              assert(crc.has("allFiles") && crc.get("allFiles").isArray)
              val active = (if (version == baseVersion) adds.slice(1, 4) else adds.drop(1))
                .map(_.copy(dataChange = false))
              val encoded = crc.get("allFiles").elements().asScala.map { node =>
                Action.parse("{\"add\":" + node.toString + "}").asInstanceOf[AddFile]
              }.toVector
              // The optional tags map can round-trip through Parquet as empty, not absent.
              val normalized = encoded.map(a => a.copy(tags = a.tags.filter(_.nonEmpty)))
              assert(normalized.sortBy(_.path) == active.sortBy(_.path))
              assert(crc.get("numFiles").asLong() == active.size)
              assert(crc.get("tableSizeBytes").asLong() == active.map(_.size).sum)
              info(s"size CRC v$version bytes=${Files.size(crcPath)} " +
                s"allFiles=${encoded.size} activeBytes=${active.map(_.size).sum}")
            }
          } else {
            // JSON 4 supports historical version lookup; absent 0..3 requires checkpoint 4.
            // JSON 5 is a real one-commit replay tail with one new AddFile.
            files.filter(p => p.getFileName.toString.matches("[0-9]{20}\\.json"))
              .filter(_.getFileName.toString.take(20).toLong < 4L).foreach(Files.delete)
            files.filter(_.getFileName.toString.endsWith(".crc")).foreach(Files.delete)
            Files.deleteIfExists(log.resolve("_last_checkpoint"))
          }
          val jsonPaths = files.filter(p => Files.exists(p) &&
            p.getFileName.toString.matches("[0-9]{20}\\.json"))
          (checkpoints ++ sidecars ++ jsonPaths).foreach { path =>
            val large = paddedFiles.contains(path) ||
              (shape == "manifest" && checkpoints.contains(path)) ||
              (shape == "json" && path == CommitLog.commitFile(tableDir, latestVersion))
            val size = Files.size(path)
            assert(if (large) size > boundary else size < boundary)
            info(s"size $shape component=${log.relativize(path)} bytes=$size " +
              s"large=$large boundary=$boundary")
          }
        }
        Seq(
          ("read_base", Some(baseVersion), None, baseRows,
            ReadResult(baseRows.size.toLong, if (isCrc) 3 else 4, 0L)),
          ("read_latest", None, None, latestRows,
            ReadResult(latestRows.size.toLong, if (isCrc) 4 else 5, 0L)),
          ("read_duplicates", None, Some("id = 21"), Seq(Row(21, null), Row(21, null)),
            ReadResult(2L, 1, if (isCrc) 3L else 4L))
        ).foreach { case (name, version, predicate, rows, expected) =>
          readSpec(t, name = Some(name), version = version.map(Long.box).orNull,
            predicate = predicate.orNull).assert { spec =>
            assert(spec.expectation == Succeeded(expected))
            assertReadRows(spec, copiedTablePath, schema, rows, s"size $shape $name")
          }
        }
        Seq(Some(baseVersion), None).foreach { version =>
          snapshotSpec(t, version = version.map(Long.box).orNull).assert { spec =>
            assert(spec.query.version.contains(version.getOrElse(latestVersion)))
            assert(spec.expectation == Succeeded(expectedSnapshot))
          }
        }
        if (isCrc) crcSpec(t, version = baseVersion, name = "crc_complete_active")
      }
    }

}
