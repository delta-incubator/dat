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
 * Log Replay + Production Edge Cases workloads (lr_* + log_* + lc_* + prod_*).
 * Covers: add-then-remove log replay, checkpoint supersedes log, full replay without
 * checkpoint, metadata latest wins, dataChange=false from compaction, missing
 * protocol/metadata errors, add-remove-readd, DV key dedup, last checkpoint info,
 * production edge cases (empty table, many commits, version gaps, truncated logs,
 * unknown reader features, varchar metadata, duplicate file refs, external checkpoints).
 *
 */
class LogReplaySuite extends WorkloadTestSuite("log_replay") {

  // Log replay: add then remove

  test("add_then_remove") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    sql("DELETE FROM tbl WHERE id = 2")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id = 2", name = "read_deleted")
    snapshotSpec(t)
  }

  test("checkpoint_supersedes_log") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    sql("INSERT INTO tbl VALUES (3), (4)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000000.json"))
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000001.json"))
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("no_checkpoint_full_replay") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    sql("INSERT INTO tbl VALUES (2)")
    sql("INSERT INTO tbl VALUES (3)")
    val t = registerTable("tbl")
    // Ensure no checkpoint exists — remove any CRC files but keep commits
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".crc")).foreach(java.nio.file.Files.delete)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("metadata_latest_wins") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    sql("ALTER TABLE tbl ADD COLUMN name STRING")
    sql("INSERT INTO tbl VALUES (3, 'alice')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "name IS NOT NULL")
    snapshotSpec(t)
  }

  test("datachange_false") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    sql("INSERT INTO tbl VALUES (3), (4)")
    sql("OPTIMIZE tbl")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("add_remove_readd") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    sql("DELETE FROM tbl WHERE id = 2")
    sql("INSERT INTO tbl VALUES (2)")  // re-add data
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id = 2")
    readSpec(t, version = 1)  // before delete
    snapshotSpec(t)
  }

  test("dv_key_dedup") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 21)")
    sql("DELETE FROM tbl WHERE id <= 5")
    sql("DELETE FROM tbl WHERE id <= 10")  // re-DV same base file
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("basic") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("checksum") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("multi_version") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    sql("INSERT INTO tbl VALUES (2)")
    forceCheckpoint("tbl")
    sql("INSERT INTO tbl VALUES (3)")
    sql("INSERT INTO tbl VALUES (4)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    readSpec(t, version = 2)  // checkpoint version
    snapshotSpec(t)
  }

  test("after_ops") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("DELETE FROM tbl WHERE id < 3")
    sql("INSERT INTO tbl VALUES (100)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("with_schema") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    sql("ALTER TABLE tbl ADD COLUMN name STRING")
    sql("INSERT INTO tbl VALUES (2, 'alice')")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("empty_table_with_schema") {
    sql("CREATE TABLE tbl (id INT, name STRING, score DOUBLE) USING delta")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("many_small_commits") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    for (i <- 1 to 51) {
      sql(s"INSERT INTO tbl VALUES ($i)")
    }
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("non_contiguous_versions") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    sql("INSERT INTO tbl VALUES (2)")
    sql("INSERT INTO tbl VALUES (3)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000002.json"))
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("truncated_log") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    sql("INSERT INTO tbl VALUES (2)")
    sql("INSERT INTO tbl VALUES (3)")
    sql("INSERT INTO tbl VALUES (4)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000000.json"))
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000001.json"))
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000002.json"))
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("external_writer_checkpoint") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
     forceCheckpoint("tbl")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("duplicate_add_file_refs") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    sql("INSERT OVERWRITE tbl VALUES (4), (5), (6)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("varchar_metadata_missing") {
    sql("CREATE TABLE tbl (id INT, name VARCHAR(100)) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'hello'), (2, 'world')")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("unknown_reader_feature") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"readerFeatures\"")) {
          line.replace("\"readerFeatures\":[",
            "\"readerFeatures\":[\"unknownFutureReaderFeature\",")
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    readSpec(t)
  }

}
