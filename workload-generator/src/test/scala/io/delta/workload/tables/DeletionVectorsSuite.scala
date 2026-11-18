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

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}

import io.delta.workload.{WorkloadContext, WorkloadTestSuite}
import io.delta.workload.deltaharness.{Format => HarnessFormat}
import io.delta.workload.engine.{RowComparison, SnapshotResolver}
import io.delta.workload.json.JsonUtil
import io.delta.workload.log.{Action, AddFile, CommitLog, DeletionVector, Metadata, Protocol,
  RemoveFile}
import io.delta.workload.model._
import io.delta.workload.tables.DeletionVectorPayloads.{payload, sidecar}

class DeletionVectorsSuite extends WorkloadTestSuite("deletion_vectors") {

  // === Deletion Vectors ===

  test("basic_delete") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    readSpec(t, predicate = "id > 3")
    snapshotSpec(t)
    snapshotSpec(t, version = 1)
  }

  test("large_table") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2000)")
    sql("DELETE FROM tbl WHERE value IN (0, 180, 300, 700, 1800)")
    sql("INSERT INTO tbl VALUES (300), (700)")
    sql("DELETE FROM tbl WHERE value IN (300, 250, 350, 900, 1353, 1567, 1800)")
    sql("INSERT INTO tbl VALUES (900), (1567)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    readSpec(t, version = 3)
    readSpec(t, version = 4)
    snapshotSpec(t)
  }

  test("partitioned") {
    sql("""CREATE TABLE tbl (id INT, partCol INT) USING delta
      PARTITIONED BY (partCol) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT), CAST(id % 10 AS INT) FROM range(200)")
    sql("DELETE FROM tbl WHERE id IN (0, 18, 30, 75, 100, 150)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    readSpec(t, predicate = "partCol = 3")
    readSpec(t, predicate = "partCol = 3 AND id > 25")
    snapshotSpec(t)
  }

  test("all_deleted") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3),(4),(5)")
    sql("DELETE FROM tbl WHERE true")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("multiple_deletes") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)")
    sql("DELETE FROM tbl WHERE id = 1")
    sql("DELETE FROM tbl WHERE id = 3")
    sql("DELETE FROM tbl WHERE id = 5")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("with_column_mapping") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie'),(4,'diana')")
    sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    sql("INSERT INTO tbl VALUES (5, 'eve')")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "full_name")))
    readSpec(t, predicate = "id > 2")
    snapshotSpec(t)
  }

  test("no_dvs") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("with_checkpoint") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e'),(6,'f')")
    sql("DELETE FROM tbl WHERE id IN (2, 5)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id > 3")
    snapshotSpec(t)
  }

  test("insert_after_delete") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    sql("INSERT INTO tbl VALUES (6,'f'),(7,'g'),(8,'h')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id > 5")
    readSpec(t, predicate = "id <= 5")
    snapshotSpec(t)
  }

  test("with_merge") {
    sql("""CREATE TABLE target (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO target VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
    sql("CREATE TABLE src (id INT, value STRING) USING delta")
    sql("INSERT INTO src VALUES (2,'B_updated'),(5,'e_new')")
    sql("""MERGE INTO target t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("target")
    readSpec(t)
    readSpec(t, version = 1)
    readSpec(t, predicate = "id > 3")
    snapshotSpec(t)
  }

  test("with_update") {
    sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
    sql("UPDATE tbl SET value = value * 100 WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    readSpec(t, predicate = "value > 100")
    readSpec(t, predicate = "value <= 50")
    snapshotSpec(t)
  }

  test("column_mapping_id") {
    sql("""CREATE TABLE tbl (id INT, category STRING, value STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'id', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'fruit','apple'),(2,'fruit','banana'),(3,'veggie','carrot'),(4,'veggie','daikon')")
    sql("DELETE FROM tbl WHERE id IN (2, 3)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "category = 'fruit'")
    readSpec(t, predicate = "category = 'veggie'")
    snapshotSpec(t)
  }

  test("partition_pruning_combined") {
    sql("""CREATE TABLE tbl (id INT, part STRING, value INT) USING delta
      PARTITIONED BY (part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40),(5,'C',50),(6,'C',60)")
    sql("DELETE FROM tbl WHERE id IN (2, 4, 6)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "part = 'A'")
    readSpec(t, predicate = "part IN ('A','B')")
    readSpec(t, predicate = "part = 'B' AND value > 20")
    readSpec(t, predicate = "part = 'C'")
    snapshotSpec(t)
  }

  test("single_row_deleted") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    sql("DELETE FROM tbl WHERE id = 1")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("predicate_on_deleted") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie'),(4,'diana')")
    sql("DELETE FROM tbl WHERE id IN (1, 3)")
    val t = registerTable("tbl")
    readSpec(t)
    // Predicate that matches ONLY deleted rows
    readSpec(t, predicate = "id = 1")
    readSpec(t, predicate = "id = 3")
    // Predicate that matches surviving rows
    readSpec(t, predicate = "id = 2")
    readSpec(t, predicate = "id = 4")
    snapshotSpec(t)
  }

  test("multi_file_delete") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // Insert in separate batches to create multiple files
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e'),(6,'f')")
    sql("INSERT INTO tbl VALUES (7,'g'),(8,'h'),(9,'i')")
    // Delete from each file
    sql("DELETE FROM tbl WHERE id IN (1, 5, 9)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    readSpec(t, version = 3)
    readSpec(t, predicate = "id > 3 AND id < 8")
    snapshotSpec(t)
  }

  test("time_travel_pre_dv") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
    sql("INSERT INTO tbl VALUES (5,'e'),(6,'f')")
    // Version 2: DV delete
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    // Read pre-DV version (should include all rows)
    readSpec(t, version = 1)
    // Read post-DV version
    readSpec(t)
    snapshotSpec(t)
  }

  test("insert_readback") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 2")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
    val t = registerTable("tbl")
    readSpec(t)
    // Only new rows
    readSpec(t, predicate = "id > 3")
    // Surviving original rows
    readSpec(t, predicate = "id <= 3")
    snapshotSpec(t)
  }

  test("column_projection") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'alice',1.1),(2,'bob',2.2),(3,'charlie',3.3),(4,'diana',4.4)")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    readSpec(t, columns = Some(Seq("id", "name")))
    readSpec(t, columns = Some(Seq("value")))
    snapshotSpec(t)
  }

  test("with_null_values") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,NULL),(3,'c'),(4,NULL),(5,'e')")
    sql("DELETE FROM tbl WHERE value IS NULL")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("projection_with_pred") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40),(5,'e',50)")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    readSpec(t, columns = Some(Seq("id", "name")), predicate = "value > 20")
    readSpec(t, columns = Some(Seq("name", "value")), predicate = "id < 4")
    snapshotSpec(t)
  }

  test("all_rows_deleted") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE true")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("checkpoint_only_read") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 2")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    // Remove JSON commit files, leaving only checkpoint
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".json")).foreach(java.nio.file.Files.delete)
    }
    snapshotSpec(t)
  }

  test("checkpoint_read") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e'),(6,'f')")
    sql("DELETE FROM tbl WHERE id IN (2, 5)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id > 3")
    snapshotSpec(t)
  }

  test("cm_partition_combo") {
    sql("""CREATE TABLE tbl (id INT, category STRING, value INT) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("""INSERT INTO tbl VALUES
      (1,'fruit',10),(2,'fruit',20),(3,'fruit',30),
      (4,'veggie',40),(5,'veggie',50),(6,'veggie',60),
      (7,'dairy',70),(8,'dairy',80)""")
    sql("DELETE FROM tbl WHERE id IN (2, 5, 8)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "category = 'fruit'")
    readSpec(t, predicate = "category = 'veggie'")
    snapshotSpec(t)
  }

  test("column_mapping_read") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'alice',100),(2,'bob',200)")
    sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    sql("INSERT INTO tbl VALUES (3,'charlie',300),(4,'diana',400),(5,'eve',500)")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "full_name")))
    readSpec(t, predicate = "value > 200")
    snapshotSpec(t)
  }

  test("err_001_checksum") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(20)")
    sql("DELETE FROM tbl WHERE id < 10")
    val t = registerTable("tbl")
    // Corrupt DV checksum by modifying last byte of DV bin files
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir).iterator().asScala
        .filter(_.getFileName.toString.contains("deletion_vector"))
        .foreach { f =>
          val bytes = java.nio.file.Files.readAllBytes(f)
          if (bytes.length > 0) { bytes(bytes.length - 1) = (bytes(bytes.length - 1) ^ 0xFF).toByte }
          java.nio.file.Files.write(f, bytes)
        }
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("err_002_missing_file") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(20)")
    sql("DELETE FROM tbl WHERE id < 10")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir).iterator().asScala
        .filter(_.getFileName.toString.contains("deletion_vector"))
        .foreach(java.nio.file.Files.delete)
    }
    readSpec(t)
  }

  test("err_003_malformed_path") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("DELETE FROM tbl WHERE id < 5")
    val t = registerTable("tbl")
    // Replace DV path in the commit with a malformed one
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000002.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      val patched = content.replace("deletion_vector_", "malformed/../../bad_dv_")
      java.nio.file.Files.write(f, patched.getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("inline_vs_ondisk") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT), CAST(id AS STRING) FROM range(2, 100)")
    sql("INSERT INTO tbl SELECT CAST(id AS INT), CAST(id AS STRING) FROM range(100, 200)")
    sql("DELETE FROM tbl WHERE id = 1")
    sql("DELETE FROM tbl WHERE id >= 50 AND id < 100")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("multiple_dvs_same_file") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j')")
    sql("DELETE FROM tbl WHERE id = 1")
    sql("DELETE FROM tbl WHERE id = 3")
    sql("DELETE FROM tbl WHERE id = 5")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("partition_pruning") {
    sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
      PARTITIONED BY (region)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1,'east',100),(2,'east',200),(3,'east',300),
      (4,'west',400),(5,'west',500),(6,'west',600),
      (7,'north',700),(8,'north',800),
      (9,'south',900)""")
    sql("DELETE FROM tbl WHERE id IN (1, 5, 9)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "region = 'east'")
    readSpec(t, predicate = "region IN ('north', 'east')")
    readSpec(t, predicate = "region = 'west' AND amount > 400")
    snapshotSpec(t)
  }

  test("special_path_chars") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    sql("DELETE FROM tbl WHERE id = 3")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("storage_type_i") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT), CAST(id AS STRING) FROM range(1, 11)")
    sql("DELETE FROM tbl WHERE id <= 2")
    sql("DELETE FROM tbl WHERE id <= 2")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("storage_type_p") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT), CAST(id AS STRING) FROM range(1, 11)")
    sql("DELETE FROM tbl WHERE id <= 2")
    sql("DELETE FROM tbl WHERE id <= 2")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("storage_type_u") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT), CAST(id AS STRING) FROM range(1, 11)")
    sql("DELETE FROM tbl WHERE id <= 3")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("with_cm_partitioned") {
    sql("""CREATE TABLE tbl (id INT, dept STRING, salary INT) USING delta
      PARTITIONED BY (dept)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'id',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("""INSERT INTO tbl VALUES
      (1,'eng',100),(2,'eng',200),(3,'eng',300),
      (4,'sales',400),(5,'sales',500),
      (6,'hr',600),(7,'hr',700)""")
    sql("DELETE FROM tbl WHERE id IN (2, 5, 7)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "dept = 'eng'")
    readSpec(t, predicate = "dept = 'hr'")
    readSpec(t, columns = Some(Seq("id", "salary")))
    snapshotSpec(t)
  }

  test("with_offset") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e'),(6,'f')")
    sql("INSERT INTO tbl VALUES (7,'g'),(8,'h'),(9,'i')")
    sql("DELETE FROM tbl WHERE id IN (1, 4, 7)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("DV-001") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2000)")
    sql("DELETE FROM tbl WHERE value IN (0, 180, 300, 700, 1800)")
    sql("INSERT INTO tbl VALUES (300), (700)")
    sql("DELETE FROM tbl WHERE value IN (300, 250, 350, 900, 1353, 1567, 1800)")
    sql("INSERT INTO tbl VALUES (900), (1567)")
    val t = registerTable("tbl")
    readSpec(t, version = 0, name = Some("version_0"))
    readSpec(t, version = 4, name = Some("version_4"))
    snapshotSpec(t)
  }

  test("DV-002") {
    sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.feature.allowColumnDefaults' = 'supported')""")
    // Insert 2000 rows: id 0..1999, name='name_{id}', status defaults to 'active'
    sql("""INSERT INTO tbl (id, name)
      SELECT CAST(id AS INT), CONCAT('name_', CAST(id AS STRING)) FROM range(2000)""")
    // Delete specific ids
    sql("DELETE FROM tbl WHERE id IN (0, 18, 30, 75, 100, 150, 300, 500, 700, 1000, 1500, 1800)")
    // Insert more rows
    sql("""INSERT INTO tbl (id, name)
      SELECT CAST(id AS INT), CONCAT('name_', CAST(id AS STRING)) FROM range(2000, 2500)""")
    // Second round of deletes
    sql("DELETE FROM tbl WHERE id IN (300, 350, 400, 900, 1200, 1353, 1567)")
    // Insert replacements
    sql("""INSERT INTO tbl (id, name)
      SELECT CAST(id AS INT), CONCAT('name_', CAST(id AS STRING)) FROM range(2500, 3000)""")
    val t = registerTable("tbl")
    readSpec(t, version = 0, name = Some("version_0"))
    readSpec(t, version = 4, name = Some("version_4"))
    snapshotSpec(t)
  }

  test("DV-003") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2000)")
    sql("DELETE FROM tbl WHERE value IN (0, 180, 300, 700, 1800)")
    sql("INSERT INTO tbl VALUES (300), (700)")
    sql("DELETE FROM tbl WHERE value IN (300, 250, 350, 900, 1353, 1567, 1800)")
    sql("INSERT INTO tbl VALUES (900), (1567)")
    val t = registerTable("tbl")
    snapshotSpec(t)
  }

  test("DV-004") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2000)")
    sql("DELETE FROM tbl WHERE value IN (0, 180, 300, 700, 1800)")
    sql("INSERT INTO tbl VALUES (300), (700)")
    sql("DELETE FROM tbl WHERE value IN (300, 250, 350, 900, 1353, 1567, 1800)")
    sql("INSERT INTO tbl VALUES (900), (1567)")
    val t = registerTable("tbl")
    snapshotSpec(t)
  }

  test("DV-005a") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2000)")
    sql("DELETE FROM tbl WHERE value IN (0, 180, 300, 700, 1800)")
    sql("INSERT INTO tbl VALUES (300), (700)")
    sql("DELETE FROM tbl WHERE value IN (300, 250, 350, 900, 1353, 1567, 1800)")
    sql("INSERT INTO tbl VALUES (900), (1567)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("count"))
    snapshotSpec(t)
  }

  test("DV-005b") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'test')")
    val t = registerTable("tbl")
    readSpec(t, name = Some("count"))
    snapshotSpec(t)
  }

  test("DV-006") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // Create 1000 rows (id 0..999)
    sql("INSERT INTO tbl SELECT id FROM range(1000)")
    // Enable DVs explicitly (table was created without them in the original)
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    // Delete even ids < 200
    sql("DELETE FROM tbl WHERE id % 2 = 0 AND id < 200")
    val t = registerTable("tbl")
    readSpec(t, name = Some("after_delete"))
    snapshotSpec(t)
  }

  test("DV-007") {
    sql("""CREATE TABLE tbl (value LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(50)")
    // First delete
    sql("DELETE FROM tbl WHERE value IN (0, 10, 20, 30, 40)")
    // Second delete (on table already containing DVs)
    sql("DELETE FROM tbl WHERE value IN (49, 29, 7, 8, 17, 36)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("after_additional_delete"))
    snapshotSpec(t)
  }

  test("DV-008") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'test')")
    val t = registerTable("tbl")
    readSpec(t, name = Some("table2_latest"))
    snapshotSpec(t)
  }

  test("DV-009") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'test')")
    val t = registerTable("tbl")
    readSpec(t, name = Some("table2_latest_v1"))
    readSpec(t, version = 0, name = Some("table2_version_0"))
    snapshotSpec(t)
  }

  test("DV-010") {
    sql("""CREATE TABLE tbl (value LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(20)")
    sql("DELETE FROM tbl WHERE value IN (0, 5, 10, 15)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 24)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("after_insert"))
    snapshotSpec(t)
  }

  test("DV-011") {
    sql("""CREATE TABLE tbl (part INT, col1 INT, col2 STRING) USING delta
      PARTITIONED BY (part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // Insert 50 rows: 10 partitions x 5 rows each
    sql("""INSERT INTO tbl
      SELECT
        CAST(id % 10 AS INT) as part,
        CAST(id AS INT) as col1,
        CONCAT('foo', CAST(id % 5 AS STRING)) as col2
      FROM range(50)""")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    sql("DELETE FROM tbl WHERE col1 = 2")
    val t = registerTable("tbl")
    readSpec(t, name = Some("after_delete"))
    readSpec(t, predicate = "col1 = 2", name = Some("filter_col1_eq_2"))
    snapshotSpec(t)
  }

  test("DV-012") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(200)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    sql("DELETE FROM tbl WHERE id % 2 = 0 AND id < 20")
    val t = registerTable("tbl")
    readSpec(t, name = Some("after_delete"))
    snapshotSpec(t)
  }

  test("DV-013") {
    sql("""CREATE TABLE tbl (value LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("DELETE FROM tbl WHERE value IN (0, 9)")
    // Create source for MERGE: values 1-8 (matched) + 10001-10008 (not matched)
    sql("CREATE TABLE src (value LONG) USING delta")
    sql("INSERT INTO src SELECT id FROM range(1, 9)")
    sql("INSERT INTO src SELECT id + 10001 FROM range(8)")
    sql("""MERGE INTO tbl t USING src s ON t.value = s.value
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t, name = Some("after_merge"))
    snapshotSpec(t)
  }

  test("DV-014") {
    sql("""CREATE TABLE tbl (value LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("DELETE FROM tbl WHERE value IN (0, 9)")
    sql("UPDATE tbl SET value = -1 WHERE value = 1")
    val t = registerTable("tbl")
    readSpec(t, name = Some("after_update"))
    snapshotSpec(t)
  }

  test("DV-015") {
    sql("""CREATE TABLE tbl (value LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("DELETE FROM tbl WHERE value IN (0, 9)")
    // Trying to update a deleted row - should be a no-op
    sql("UPDATE tbl SET value = -1 WHERE value = 0")
    val t = registerTable("tbl")
    readSpec(t, name = Some("after_noop_update"))
    snapshotSpec(t)
  }

  test("DV-016") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("DELETE FROM tbl WHERE id IN (1, 8)")
    sql("UPDATE tbl SET id = -1 WHERE id = 0")
    // MERGE: source has values matching remaining rows
    sql("CREATE TABLE src (value LONG) USING delta")
    sql("INSERT INTO src SELECT id FROM range(-1, 10)")
    sql("""MERGE INTO tbl t USING src s ON t.id = s.value
      WHEN MATCHED THEN UPDATE SET id = t.id
      WHEN NOT MATCHED THEN INSERT (id) VALUES (s.value)""")
    sql("DELETE FROM tbl WHERE id = 4")
    val t = registerTable("tbl")
    readSpec(t, version = 0, name = Some("version_0_initial"))
    readSpec(t, version = 1, name = Some("version_1_after_delete"))
    readSpec(t, version = 2, name = Some("version_2_after_update"))
    readSpec(t, version = 3, name = Some("version_3_after_merge"))
    readSpec(t, version = 4, name = Some("version_4_final"))
    snapshotSpec(t)
  }

  test("DV-017") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // WARNING: This generates ~2.1 billion rows. Only run if you have sufficient resources.
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2145386174)")
    // The original table has a DV that removes ~50000 rows
    sql("DELETE FROM tbl WHERE value >= 0 AND value < 50000")
    val t = registerTable("tbl")
    readSpec(t, name = Some("full_table_count"))
    snapshotSpec(t)
  }

  test("DV-018") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_no_dv_table"))
    snapshotSpec(t)
  }

  // Inline and UUID-relative deletion vectors with the same engine-written bitmap.
  Seq("inline", "uuid_relative").foreach { form =>
    test(s"e2e_42_$form") {
      sql("""CREATE TABLE tbl (id INT, value STRING) USING delta TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true', 'delta.checkpointInterval' = '1000',
        'delta.checkpointPolicy' = 'classic')""")
      sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
        (-7, 'remove'), (0, 'keep'), (4, CAST(NULL AS STRING)), (4, CAST(NULL AS STRING)),
        (9, 'remove'), (12, 'keep'), (18, CAST(NULL AS STRING)), (23, 'edge'),
        (31, 'keep'), (44, CAST(NULL AS STRING)), (57, 'remove'), (88, 'last') AS v(id, value)""")
      sql("DELETE FROM tbl WHERE id IN (-7, 9, 57)")
      sql("DELETE FROM tbl WHERE id = 4")
      val table = registerTable("tbl")
      val schema = new StructType().add("id", IntegerType).add("value", StringType)
      val baseRows = Seq(Row(-7, "remove"), Row(0, "keep"), Row(4, null), Row(4, null),
        Row(9, "remove"), Row(12, "keep"), Row(18, null), Row(23, "edge"),
        Row(31, "keep"), Row(44, null), Row(57, "remove"), Row(88, "last"))
      val firstRows = baseRows.filterNot(r => Set(-7, 9, 57).contains(r.getInt(0)))
      val latestRows = firstRows.filterNot(_.getInt(0) == 4)
      val original = (0L to 3L).map(v => v -> CommitLog.read(table.sourcePath, v)).toMap
      val metadata = original(0L).collect { case m: Metadata => m }.ensuring(_.size == 1).head
      val protocol = original(0L).collect { case p: Protocol => p }.ensuring(_.size == 1).head
      assert(DataType.fromJson(metadata.schemaString) == schema)
      assert(metadata.partitionColumns.isEmpty)
      assert(metadata.configuration == Map("delta.enableDeletionVectors" -> "true",
        "delta.checkpointInterval" -> "1000", "delta.checkpointPolicy" -> "classic"))
      assert(protocol == Protocol(3, 7, Some(Set("deletionVectors")),
        Some(Set("appendOnly", "deletionVectors", "invariants"))))
      info("DV original protocol: " + protocol.toJson)
      info("DV original metadata: " + metadata.toJson)
      val adds = (1L to 3L).map { version =>
        val found = original(version).collect { case a: AddFile => a }
        assert(found.size == 1)
        info(s"DV original add v$version: " + found.head.toJson)
        version -> found.head
      }.toMap
      assert(adds.values.map(_.path).toSet.size == 1) // Deletes must retain the data file.
      assert(adds(1L).deletionVector.isEmpty)
      val physical = spark.read.parquet(table.sourcePath.resolve(adds(1L).path).toString)
      assert(physical.schema == schema)
      RowComparison.assertRowsEqual(spark.createDataFrame(baseRows.asJava, schema), physical,
        s"$form physical input")
      val physicalRows = physical.collect().toSeq
      val sourceDvs = (2L to 3L).map(v => v -> adds(v).deletionVector.get).toMap
      sourceDvs.foreach { case (version, dv) =>
        assert(dv.storageType == "u")
        assert(dv.cardinality == (if (version == 2L) 3L else 5L))
        val expectedPositions = physicalRows.zipWithIndex.collect {
          case (row, index) if Set(-7, 9, 57).contains(row.getInt(0)) ||
            (version == 3L && row.getInt(0) == 4) => index.toLong
        }
        assert(RoaringBitmapArray.readFrom(payload(table.sourcePath, dv)).toArray.toSeq ==
          expectedPositions)
      }
      val converted = sourceDvs.values.map { dv =>
        val bytes = payload(table.sourcePath, dv)
        // Use the verified OSS helper with exact engine-generated payload bytes.
        val inline = DeletionVectorDescriptor.inlineInLog(bytes, dv.cardinality)
        assert(inline.storageType == "i" && inline.offset.isEmpty)
        assert(inline.sizeInBytes == bytes.length && inline.cardinality == dv.cardinality)
        assert(inline.inlineData.sameElements(bytes))
        dv -> DeletionVector(inline.storageType, inline.pathOrInlineDv, inline.offset,
          inline.sizeInBytes, inline.cardinality)
      }.toMap
      if (form == "inline") {
        (2L to 3L).foreach { version =>
          modifyCommitActions(table, version) { actions =>
            actions.map {
              case add: AddFile => add.copy(deletionVector = add.deletionVector.map(converted))
              // The second delete removes the first DV identity; convert that tombstone too.
              case remove: RemoveFile =>
                remove.copy(deletionVector = remove.deletionVector.map(converted))
              case other => other
            }
          }
        }
      }
      val expectedAdds = adds.map { case (version, add) =>
        version -> (if (form == "inline") {
          add.copy(deletionVector = add.deletionVector.map(converted))
        } else add)
      }
      var copiedTable: Path = null
      mutateTable(table) { dir =>
        copiedTable = dir
        val sidecars = sourceDvs.values.map(dv => sidecar(dir, dv)).toSet
        if (form == "inline") sidecars.foreach(Files.delete)
        else sidecars.foreach(p => assert(Files.isRegularFile(p)))
        // Version checksums embed the original descriptors and become stale after conversion.
        val logs = Files.list(dir.resolve("_delta_log"))
        try logs.iterator().asScala.filter(_.getFileName.toString.endsWith(".crc"))
          .foreach(Files.delete)
        finally logs.close()
        (1L to 3L).foreach { version =>
          val actions = CommitLog.read(dir, version)
          assert(actions.collect { case a: AddFile => a } == Seq(expectedAdds(version)))
          expectedAdds(version).deletionVector.foreach { dv =>
            assert(dv.storageType == (if (form == "inline") "i" else "u"))
            assert(dv.cardinality == (if (version == 2L) 3L else 5L))
            assert(payload(dir, dv).sameElements(payload(table.sourcePath, sourceDvs(version))))
            assert(RoaringBitmapArray.readFrom(payload(dir, dv)).cardinality == dv.cardinality)
          }
        }
        val oldIdentity = CommitLog.read(dir, 3L).collect { case r: RemoveFile => r }
        assert(oldIdentity.size == 1)
        assert(oldIdentity.head.path == adds(1L).path)
        assert(oldIdentity.head.deletionVector == expectedAdds(2L).deletionVector)
        assert(Files.size(dir.resolve(adds(1L).path)) == adds(1L).size)
        if (form == "inline") assert(sidecars.forall(p => !Files.exists(p)))
      }
      val snapshot = SnapshotResult(
        ProtocolInfo(3, 7, Some(Seq("deletionVectors")),
          Some(Seq("appendOnly", "deletionVectors", "invariants"))),
        MetadataInfo(metadata.id, metadata.name, metadata.description,
          HarnessFormat("parquet", Map.empty), metadata.schemaString, Seq.empty,
          metadata.configuration, metadata.createdTime))
      Seq[java.lang.Long](1L, 2L, null).foreach { version =>
        snapshotSpec(table, version = version).assert { spec =>
          assert(spec.query == SnapshotQuery(version = Some(Option(version).fold(3L)(_.toLong))))
          assert(spec.expectation == Succeeded(snapshot))
        }
      }
      val projected = new StructType().add("value", StringType)
      val reordered = projected.add("id", IntegerType)
      val filter = "id IN (4, 12, 18, 57)"
      val cases = Seq(
        ("read_base", ReadQuery(version = Some(1L)), schema, baseRows),
        ("read_first_delete", ReadQuery(version = Some(2L)), schema, firstRows),
        ("read_latest", ReadQuery(), schema, latestRows),
        ("read_v2_filtered_projected", ReadQuery(version = Some(2L),
          predicate = Some(filter), columns = Some(Seq("value", "id"))), reordered,
          Seq(Row(null, 4), Row(null, 4), Row("keep", 12), Row(null, 18))),
        ("read_v3_filtered_projected", ReadQuery(version = Some(3L),
          predicate = Some(filter), columns = Some(Seq("value", "id"))), reordered,
          Seq(Row("keep", 12), Row(null, 18))),
        ("read_projected", ReadQuery(columns = Some(Seq("value"))), projected,
          Seq(Row("keep"), Row("keep"), Row(null), Row("edge"), Row("keep"), Row(null),
            Row("last"))),
        ("read_deleted_only", ReadQuery(predicate = Some("id IN (-7, 4, 9, 57)")), schema,
          Seq.empty[Row]))
      cases.foreach { case (name, query, expectedSchema, rows) =>
        readSpec(table, name = Some(name), version = query.version.map(Long.box).orNull,
          predicate = query.predicate.orNull, columns = query.columns).assert { spec =>
          assert(spec.query == query)
          // All predicates overlap physical min/max; the single data AddFile remains scanned.
          assert(spec.expectation == Succeeded(ReadResult(rows.size.toLong, 1, 0L)))
          val actual = SnapshotResolver.applyFilters(
            SnapshotResolver.buildDeltaReader(spark, copiedTable, query.version, query.timestamp),
            query.predicate, query.columns)
          assert(actual.schema == expectedSchema)
          RowComparison.assertRowsEqual(spark.createDataFrame(rows.asJava, expectedSchema), actual,
            s"$form $name")
          val fixture = outputDir.resolve(WorkloadContext.current.workloadName)
          val specName = s"${fixture.getFileName}_$name"
          val captured = spark.read.parquet(
            fixture.resolve(s"expected/$specName/expected_metadata").toString)
          val scanned = captured.collect().toSeq.map { row =>
            val node = JsonUtil.mapper.readTree(row.getString(0))
            val wrapped = if (node.has("add")) node.toString else s"""{"add":$node}"""
            Action.parse(wrapped)
          }
          // OSS InMemoryLogReplay stores active AddFiles with dataChange=false;
          // ReadCapture obtains these snapshot actions through DeltaSparkHarness.allFiles.
          assert(scanned == Seq(expectedAdds(query.version.getOrElse(3L))
            .copy(dataChange = false)))
        }
      }
    }
  }

}
