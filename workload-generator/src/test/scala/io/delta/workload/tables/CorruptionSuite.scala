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

class CorruptionSuite extends WorkloadTestSuite("corruption") {

  // === Corrupt Tables ===

  test("missing_file") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(100)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir).iterator().asScala
        .filter(_.toString.endsWith(".parquet")).take(1).foreach(java.nio.file.Files.delete)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("truncated_commit") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000001.json")
      if (java.nio.file.Files.exists(f)) {
        val bytes = java.nio.file.Files.readAllBytes(f)
        java.nio.file.Files.write(f, java.util.Arrays.copyOf(bytes, 10))
      }
    }
    readSpec(t)
  }

  test("no_crc") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".crc")).foreach(java.nio.file.Files.delete)
    }
    readSpec(t)
    readSpec(t, predicate = "id < 5")
    snapshotSpec(t)
  }

  test("empty_crc") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val crc = dir.resolve("_delta_log/00000000000000000000.crc")
      java.nio.file.Files.write(crc, Array.emptyByteArray)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("bad_stats") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    val t = registerTable("tbl")
    modifyCommitActions(t, version = 1) { actions =>
      actions.map { case ("add", node) =>
        node.put("stats", """{"numRecords":999}"""); ("add", node)
        case other => other
      }
    }
    readSpec(t)
    readSpec(t, predicate = "id > 1")
    snapshotSpec(t)
  }

  test("version_gap") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    sql("INSERT INTO tbl VALUES (2)")
    sql("INSERT INTO tbl VALUES (3)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.delete(dir.resolve("_delta_log/00000000000000000002.json"))
    }
    readSpec(t)
  }

  test("no_protocol") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.filterNot(_.contains("\"protocol\""))
      java.nio.file.Files.write(f, lines.asJava)
      // Delete all CRC files - they cache protocol/metadata state
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".crc")).foreach(java.nio.file.Files.delete)
    }
    readSpec(t)
  }

  test("no_metadata") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.filterNot(_.contains("\"metaData\""))
      java.nio.file.Files.write(f, lines.asJava)
      // Delete all CRC files - they cache protocol/metadata state
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".crc")).foreach(java.nio.file.Files.delete)
    }
    readSpec(t)
  }

  test("stale_last_checkpoint") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.write(dir.resolve("_delta_log/_last_checkpoint"), """{"version":999}""".getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("invalid_last_checkpoint") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.write(dir.resolve("_delta_log/_last_checkpoint"), "NOT VALID JSON".getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("empty_delta_log") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala.foreach(java.nio.file.Files.delete)
    }
    readSpec(t)
  }

  test("zero_byte_commit") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.write(dir.resolve("_delta_log/00000000000000000000.json"), Array.emptyByteArray)
      // Delete all CRC files - they cache protocol/metadata state
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".crc")).foreach(java.nio.file.Files.delete)
    }
    readSpec(t)
  }

  test("dv_garbled") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(100)")
    sql("DELETE FROM tbl WHERE id < 10")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir).iterator().asScala
        .filter(_.getFileName.toString.contains("deletion_vector"))
        .foreach(f => java.nio.file.Files.write(f, Array[Byte](0,1,2,3)))
    }
    readSpec(t)
  }

  test("unknown_action") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      java.nio.file.Files.write(f, (content.trim + "\n" + """{"unknownAction":{"key":"val"}}""" + "\n").getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("parquet") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir).iterator().asScala
        .filter(_.toString.endsWith(".parquet"))
        .take(1).foreach { f =>
          val bytes = java.nio.file.Files.readAllBytes(f)
          java.nio.file.Files.write(f, java.util.Arrays.copyOf(bytes, 10))
        }
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("duplicate_metadata") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      val lines = content.trim.split("\n")
      val mdLine = lines.find(_.contains("\"metaData\"")).getOrElse("")
      java.nio.file.Files.write(f, (content.trim + "\n" + mdLine + "\n").getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("duplicate_protocol") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      val lines = content.trim.split("\n")
      val protoLine = lines.find(_.contains("\"protocol\"")).getOrElse("")
      java.nio.file.Files.write(f, (content.trim + "\n" + protoLine + "\n").getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("gap_in_versions") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.delete(dir.resolve("_delta_log/00000000000000000002.json"))
    }
    readSpec(t)
  }

  test("invalid_json") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.write(dir.resolve("_delta_log/00000000000000000000.json"),
        "NOT VALID JSON{{{".getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("missing_data_file") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir).iterator().asScala
        .filter(_.toString.endsWith(".parquet")).foreach(java.nio.file.Files.delete)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("missing_delta_log") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val logDir = dir.resolve("_delta_log")
      java.nio.file.Files.list(logDir).iterator().asScala.foreach(java.nio.file.Files.delete)
      java.nio.file.Files.delete(logDir)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("missing_metadata") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.filterNot(_.contains("\"metaData\""))
      java.nio.file.Files.write(f, lines.asJava)
      // Delete all CRC files - they cache protocol/metadata state
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".crc")).foreach(java.nio.file.Files.delete)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("missing_protocol") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.filterNot(_.contains("\"protocol\""))
      java.nio.file.Files.write(f, lines.asJava)
      // Delete all CRC files - they cache protocol/metadata state
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".crc")).foreach(java.nio.file.Files.delete)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("only_remove_file") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    // Inject a v1 commit that has only remove actions
    mutateTable(t) { dir =>
      val v0 = new String(java.nio.file.Files.readAllBytes(
        dir.resolve("_delta_log/00000000000000000000.json")), "UTF-8")
      val addPathPattern = """"path":"([^"]+)""".r
      val paths = addPathPattern.findAllMatchIn(v0).map(_.group(1)).toSeq
      val removes = paths.map(p =>
        s"""{"remove":{"path":"$p","deletionTimestamp":${System.currentTimeMillis()},"dataChange":true}}"""
      ).mkString("\n")
      val ci = s"""{"commitInfo":{"timestamp":${System.currentTimeMillis()},"operation":"DELETE","operationParameters":{},"isBlindAppend":false}}"""
      java.nio.file.Files.write(dir.resolve("_delta_log/00000000000000000001.json"),
        (removes + "\n" + ci + "\n").getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("unknown_action_types") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      java.nio.file.Files.write(f,
        (content.trim + "\n" + """{"unknownAction":{"key":"val"}}""" + "\n").getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("crc_empty") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val crc = dir.resolve("_delta_log/00000000000000000000.crc")
      java.nio.file.Files.write(crc, Array.emptyByteArray)
    }
    readSpec(t)
    readSpec(t, predicate = "id >= 5")
    snapshotSpec(t)
  }

  test("crc_negative_counts") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val crc = dir.resolve("_delta_log/00000000000000000000.crc")
      if (java.nio.file.Files.exists(crc)) {
        val content = new String(java.nio.file.Files.readAllBytes(crc), "UTF-8")
        val patched = content.replaceAll(""""numFiles":\d+""", """"numFiles":-1""")
        java.nio.file.Files.write(crc, patched.getBytes)
      }
    }
    readSpec(t)
    readSpec(t, predicate = "id > 7")
    snapshotSpec(t)
  }

  test("crc_no_metadata") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val crc = dir.resolve("_delta_log/00000000000000000000.crc")
      java.nio.file.Files.write(crc, """{"tableSizeBytes":0}""".getBytes)
    }
    readSpec(t)
    readSpec(t, predicate = "id < 3")
    snapshotSpec(t)
  }

  test("crc_txnid_mismatch") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val crc = dir.resolve("_delta_log/00000000000000000000.crc")
      if (java.nio.file.Files.exists(crc)) {
        val content = new String(java.nio.file.Files.readAllBytes(crc), "UTF-8")
        val patched = content.replaceAll(""""txnId":"[^"]*"""", """"txnId":"00000000-0000-0000-0000-000000000000"""")
        java.nio.file.Files.write(crc, patched.getBytes)
      }
    }
    readSpec(t)
    readSpec(t, predicate = "id >= 5")
    snapshotSpec(t)
  }

  test("crc_wrong_numfiles") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val crc = dir.resolve("_delta_log/00000000000000000000.crc")
      if (java.nio.file.Files.exists(crc)) {
        val content = new String(java.nio.file.Files.readAllBytes(crc), "UTF-8")
        val patched = content.replaceAll(""""numFiles":\d+""", """"numFiles":999""")
        java.nio.file.Files.write(crc, patched.getBytes)
      }
    }
    readSpec(t)
    readSpec(t, predicate = "id < 5")
    snapshotSpec(t)
  }

  test("incomplete_multipart_checkpoint") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.checkpointInterval' = '5')""")
    for (i <- 0 until 20) sql(s"INSERT INTO tbl SELECT id FROM range(${i*10}, ${(i+1)*10})")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id < 50")
    snapshotSpec(t)
  }

  test("last_checkpoint_checksum_mismatch") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val lc = dir.resolve("_delta_log/_last_checkpoint")
      if (java.nio.file.Files.exists(lc)) {
        val content = new String(java.nio.file.Files.readAllBytes(lc), "UTF-8")
        val patched = content.replaceAll(""""checksum":"[^"]*"""", """"checksum":"badchecksum"""")
        java.nio.file.Files.write(lc, patched.getBytes)
      }
    }
    readSpec(t)
    readSpec(t, predicate = "id >= 5")
    snapshotSpec(t)
  }

  test("malformed_last_checkpoint") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.write(dir.resolve("_delta_log/_last_checkpoint"), "NOT VALID JSON".getBytes)
    }
    readSpec(t)
    readSpec(t, predicate = "id >= 5")
    snapshotSpec(t)
  }

  test("malformed_stats_json") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      val patched = content.replaceAll(""""stats":"[^"]*"""", """"stats":"{invalid json"""")
      java.nio.file.Files.write(f, patched.getBytes)
    }
    readSpec(t)
    readSpec(t, predicate = "id < 5")
    snapshotSpec(t)
  }

  test("missing_last_checkpoint") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".crc")).foreach(java.nio.file.Files.delete)
      val lc = dir.resolve("_delta_log/_last_checkpoint")
      if (java.nio.file.Files.exists(lc)) java.nio.file.Files.delete(lc)
    }
    readSpec(t)
    readSpec(t, predicate = "id < 3")
    snapshotSpec(t)
  }

  test("stale_checkpoint_extra_files") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    sql("DELETE FROM tbl WHERE id >= 20")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 2)
    readSpec(t, version = 3)
    snapshotSpec(t)
  }

  test("truncated_commit_json") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val bytes = java.nio.file.Files.readAllBytes(f)
      java.nio.file.Files.write(f, java.util.Arrays.copyOf(bytes, 10))
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("wrong_last_checkpoint") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.write(dir.resolve("_delta_log/_last_checkpoint"),
        """{"version":999,"size":1}""".getBytes)
    }
    readSpec(t)
    readSpec(t, predicate = "id < 5")
    snapshotSpec(t)
  }

  test("err_add_and_remove_same_path_dv") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 2")
    val t = registerTable("tbl")
    // Duplicate the add action into remove with same DV info
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000002.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      val addLine = content.split("\n").find(_.contains("\"add\"")).getOrElse("")
      val removeLine = addLine.replace("\"add\"", "\"remove\"")
      java.nio.file.Files.write(f, (content.trim + "\n" + removeLine + "\n").getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("err_dv_invalid_storage_type") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 2")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000002.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      val patched = content.replaceAll(""""storageType":"[iup]"""", """"storageType":"x"""")
      java.nio.file.Files.write(f, patched.getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("err_duplicate_add_same_version") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      val addLine = content.split("\n").find(_.contains("\"add\"")).getOrElse("")
      java.nio.file.Files.write(f, (content.trim + "\n" + addLine + "\n").getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("err_missing_version_0") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.delete(dir.resolve("_delta_log/00000000000000000000.json"))
    }
    readSpec(t)
  }

  test("err_schema_empty") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      val patched = content.replaceAll(""""schemaString":"[^"]*"""", """"schemaString":""""")
      java.nio.file.Files.write(f, patched.getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("err_schema_invalid_json") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      val patched = content.replaceAll(""""schemaString":"[^"]*"""", """"schemaString":"NOT VALID JSON{{{"""")
      java.nio.file.Files.write(f, patched.getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

}
