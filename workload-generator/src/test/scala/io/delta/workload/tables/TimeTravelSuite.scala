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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}

import io.delta.workload.{WorkloadContext, WorkloadTestSuite}
import io.delta.workload.deltaharness.{DeltaHarness, Format => HarnessFormat}
import io.delta.workload.engine.{RowComparison, SnapshotResolver}
import io.delta.workload.json.JsonUtil
import io.delta.workload.log.{AddFile, CommitLog, Metadata, Protocol, RemoveFile}
import io.delta.workload.model._

class TimeTravelSuite extends WorkloadTestSuite("time_travel") {

  test("versions") {
    sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'v1')")
    sql("INSERT INTO tbl VALUES (2, 'v2')")
    sql("INSERT INTO tbl VALUES (3, 'v3')")
    sql("UPDATE tbl SET val = 'updated' WHERE id = 1")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    readSpec(t, version = 3)
    readSpec(t, version = 4)
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
  }

  test("timestamps") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2)")
    sql("INSERT INTO tbl VALUES (3),(4)")
    val t = registerTable("tbl")
    val ts1 = t.getTimestampForVersion(1)
    val ts2 = t.getTimestampForVersion(2)
    readSpec(t, timestamp = ts1, name = Some("read_ts_v1"))
    readSpec(t, timestamp = ts2, name = Some("read_ts_v2"))
    readSpec(t, timestamp = SnapshotResolver.parseTimestamp(spark, "2099-01-01 00:00:00.000"),
      name = Some("read_ts_future"))
    readSpec(t, timestamp = SnapshotResolver.parseTimestamp(spark, "1970-01-01 00:00:00.000"),
      name = Some("read_ts_epoch"))
    snapshotSpec(t, timestamp = ts1)
    snapshotSpec(t)
  }

  test("schema_change") {
    sql("CREATE TABLE tbl (id BIGINT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("ALTER TABLE tbl ADD COLUMNS (name STRING)")
    sql("INSERT INTO tbl SELECT id, 'name' FROM range(10, 20)")
    val t = registerTable("tbl")
    readSpec(t, version = 1)
    readSpec(t)
    val N = 3L
    for (v <- 0L to N) snapshotSpec(t, version = v)
  }

  test("dv") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    sql("INSERT INTO tbl VALUES (6,'f'),(7,'g')")
    val t = registerTable("tbl")
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    readSpec(t)
    readSpec(t, version = 2, predicate = "id > 2")
    snapshotSpec(t)
  }

  test("bad_version") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    readSpec(t, version = 999)
    snapshotSpec(t)
  }

  test("negative_version") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    readSpec(t, version = -1)
    snapshotSpec(t)
  }

  test("deleted_version") {
    sql("CREATE TABLE tbl (id BIGINT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.delete(dir.resolve("_delta_log/00000000000000000000.json"))
    }
    readSpec(t, version = 0)
    snapshotSpec(t)
  }

  test("checkpoint") {
    sql("CREATE TABLE tbl (id BIGINT) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '5')")
    for (i <- 1 to 8) sql(s"INSERT INTO tbl SELECT id FROM range(${(i-1)*10}, ${i*10})")
    val t = registerTable("tbl")
    readSpec(t, version = 3)
    readSpec(t, version = 5)
    readSpec(t, version = 8)
    snapshotSpec(t)
  }

  test("partition_filter") {
    sql("CREATE TABLE tbl (id BIGINT, part BIGINT) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl SELECT id, id % 4 FROM range(20)")
    sql("INSERT INTO tbl SELECT id, id % 4 FROM range(20, 40)")
    val t = registerTable("tbl")
    readSpec(t, version = 1, predicate = "part = 0")
    readSpec(t, version = 2, predicate = "part = 0")
    readSpec(t)
    snapshotSpec(t)
  }

  test("column_mapping") {
    sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'before')")
    sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    sql("INSERT INTO tbl VALUES (2,'after')")
    val t = registerTable("tbl")
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    readSpec(t)
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
    snapshotSpec(t, version = 2)
    snapshotSpec(t, version = 3)
  }

  test("column_defaults") {
    sql("""CREATE TABLE tbl (id INT, value STRING DEFAULT 'default_val') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    // version 0: empty table
    sql("INSERT INTO tbl (id) VALUES (1)")
    // version 1: row with default
    sql("INSERT INTO tbl VALUES (2, 'explicit')")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t)
    snapshotSpec(t)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    readSpec(t, timestamp = ts0)
    readSpec(t, timestamp = ts1)
  }

  test("deleted_retention") {
    sql("CREATE TABLE tbl (id BIGINT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    val t = registerTable("tbl")
    // Delete version 1 commit file to simulate retention cleanup
    mutateTable(t) { dir =>
      java.nio.file.Files.delete(dir.resolve("_delta_log/00000000000000000001.json"))
    }
    readSpec(t, version = 1)
    readSpec(t)
    snapshotSpec(t)
  }

  test("after_vacuum") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    // Overwrite data so old files become candidates for vacuum
    sql("DELETE FROM tbl WHERE id < 5")
    val t = registerTable("tbl")
    // Simulate vacuum by deleting old data files
    mutateTable(t) { dir =>
      val logDir = dir.resolve("_delta_log")
      // Read remove actions from version 3 to find files to delete
      val removePattern = """"path":"([^"]+)""".r
      val v3 = new String(java.nio.file.Files.readAllBytes(logDir.resolve("00000000000000000003.json")))
      removePattern.findAllMatchIn(v3).foreach { m =>
        val f = dir.resolve(m.group(1))
        if (java.nio.file.Files.exists(f)) java.nio.file.Files.delete(f)
      }
    }
    readSpec(t, version = 1)
    snapshotSpec(t)
  }

  test("checkpoint_between") {
    sql("CREATE TABLE tbl (id BIGINT) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '3')")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    // checkpoint at version 3
    sql("INSERT INTO tbl SELECT id FROM range(30, 40)")
    sql("INSERT INTO tbl SELECT id FROM range(40, 50)")
    val t = registerTable("tbl")
    // Read before checkpoint
    readSpec(t, version = 2)
    // Read at checkpoint
    readSpec(t, version = 3)
    // Read after checkpoint
    readSpec(t, version = 5)
    readSpec(t)
    snapshotSpec(t)
  }

  test("relation_caching") {
    sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'v1')")
    sql("INSERT INTO tbl VALUES (2,'v2')")
    val t = registerTable("tbl")
    // Read version 0 and version 1 in sequence - caching must not mix them
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    readSpec(t, timestamp = ts0)
    readSpec(t)
    snapshotSpec(t)
  }

  test("sql_syntax") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'first')")
    sql("INSERT INTO tbl VALUES (2,'second')")
    val t = registerTable("tbl")
    // Version-based reads
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    // Timestamp-based reads
    val ts0 = t.getTimestampForVersion(0)
    readSpec(t, timestamp = ts0)
    readSpec(t)
    snapshotSpec(t)
  }

  test("exact_timestamp") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2)")
    sql("INSERT INTO tbl VALUES (3),(4)")
    val t = registerTable("tbl")
    val ts0 = t.getTimestampForVersion(0)
    readSpec(t, version = 0)
    readSpec(t, timestamp = ts0)
    readSpec(t, version = 1)
    readSpec(t)
    snapshotSpec(t)
  }

  test("multi_version_scans") {
    sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    sql("INSERT INTO tbl VALUES (1,10),(2,20)")
    sql("INSERT INTO tbl VALUES (3,30),(4,40)")
    val t = registerTable("tbl")
    // Multiple scans at different versions in one "session"
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    readSpec(t, timestamp = ts0)
    readSpec(t)
    snapshotSpec(t)
  }

  test("partition_evolution") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1,'A'),(2,'B')")
    // Overwrite partition A in a single commit via replaceWhere
    spark.createDataFrame(Seq((10, "A"))).toDF("id", "part")
      .write.format("delta").mode("overwrite")
      .option("replaceWhere", "part = 'A'")
      .saveAsTable("tbl")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t)
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    readSpec(t, timestamp = ts0)
    readSpec(t, timestamp = ts1)
  }

  test("timestamp_between") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    // Force a delay so timestamps differ
    Thread.sleep(1100)
    sql("INSERT INTO tbl VALUES (2)")
    Thread.sleep(1100)
    sql("INSERT INTO tbl VALUES (3)")
    val t = registerTable("tbl")
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    val ts2 = t.getTimestampForVersion(2)
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    readSpec(t, timestamp = ts0)
    readSpec(t, timestamp = ts1)
    readSpec(t)
    snapshotSpec(t)
  }

  test("version_0_empty") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'data')")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t)
    snapshotSpec(t)
  }

  test("future_timestamp_error") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2)")
    val t = registerTable("tbl")
    readSpec(t, timestamp = SnapshotResolver.parseTimestamp(spark, "2099-12-31 23:59:59.999"))
    snapshotSpec(t)
  }

  // NOTE: the former `invalid_timestamp_error` test (timestamp = "not-a-timestamp") is gone:
  // the typed `Instant` DSL makes a malformed timestamp string unrepresentable by construction, so
  // there is no longer a way to author that workload. `future_timestamp_error` above still covers
  // the timestamp-out-of-range error path.

  test("at_syntax") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    readSpec(t, timestamp = ts0)
    readSpec(t)
    snapshotSpec(t)
  }

  test("dv_between_versions") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    sql("INSERT INTO tbl VALUES (6,'f'),(7,'g')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    readSpec(t, version = 2, predicate = "id > 2")
    snapshotSpec(t)
  }

  test("non_existent_version") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    val t = registerTable("tbl")
    readSpec(t, version = 5)
    snapshotSpec(t)
  }

  test("nonexistent_version_error") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 15)")
    val t = registerTable("tbl")
    readSpec(t, version = 3)
    snapshotSpec(t)
  }

  test("timestamp_between_commits") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    Thread.sleep(1100)
    sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    Thread.sleep(1100)
    sql("INSERT INTO tbl SELECT id FROM range(10, 15)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    readSpec(t, timestamp = ts0)
    readSpec(t, timestamp = ts1)
    readSpec(t)
    snapshotSpec(t)
  }

  test("version_0") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 50)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 0, predicate = "id < 5")
    readSpec(t)
    snapshotSpec(t)
  }

  test("version_read") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 15)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    readSpec(t, timestamp = ts0)
    readSpec(t, timestamp = ts1)
    readSpec(t)
    snapshotSpec(t)
  }

  test("deleted_version_retention_error") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    sql("INSERT INTO tbl SELECT id FROM range(30, 40)")
    sql("INSERT INTO tbl SELECT id FROM range(40, 50)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.delete(dir.resolve("_delta_log/00000000000000000000.json"))
    }
    readSpec(t, version = 0)
    snapshotSpec(t)
  }

  test("schema_evolution") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("ALTER TABLE tbl ADD COLUMNS (part BIGINT)")
    sql("INSERT INTO tbl SELECT id, id % 2 FROM range(5, 10)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    readSpec(t, timestamp = ts0)
    readSpec(t, timestamp = ts1)
    readSpec(t)
    snapshotSpec(t)
    val N = 3L
    for (v <- 0L to N) snapshotSpec(t, version = v)
  }

  test("timestamp_travel") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    Thread.sleep(1100)
    sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    val t = registerTable("tbl")
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    readSpec(t, timestamp = ts0)
    readSpec(t, timestamp = ts1)
    readSpec(t)
    snapshotSpec(t)
  }

  test("timestamp_before_retention_error") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    // Checkpoint at version 2 so we can delete early commits
    forceCheckpoint("tbl")
    sql("INSERT INTO tbl SELECT id FROM range(30, 40)")
    sql("INSERT INTO tbl SELECT id FROM range(40, 50)")
    sql("INSERT INTO tbl SELECT id FROM range(50, 60)")
    val t = registerTable("tbl")
    // Record the timestamp of version 0 before deleting it
    val ts0 = t.getTimestampForVersion(0)
    // Delete version 0 JSON to simulate log cleanup / retention
    mutateTable(t) { dir =>
      val v0json = dir.resolve("_delta_log/00000000000000000000.json")
      if (java.nio.file.Files.exists(v0json)) java.nio.file.Files.delete(v0json)
    }
    // Requesting snapshot at the old timestamp should fail with
    // DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION (caught automatically by SnapshotCapture)
    snapshotSpec(t, timestamp = ts0)
    // Latest snapshot should succeed
    snapshotSpec(t)
  }

  test("oss") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // v0: 5 rows (1..5)
    sql("INSERT INTO tbl VALUES (1),(2),(3),(4),(5)")
    // v1: 5 more rows (6..10)
    sql("INSERT INTO tbl VALUES (6),(7),(8),(9),(10)")
    // v2: 5 more rows (11..15)
    sql("INSERT INTO tbl VALUES (11),(12),(13),(14),(15)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("readLatest"))
    readSpec(t, version = 0, name = Some("readV0"))
    readSpec(t, version = 1, name = Some("readV1"))
    snapshotSpec(t)
  }

  // Truncated history, VACUUM, and RESTORE.
  test("e2e_54_truncated_history") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta TBLPROPERTIES (
      'delta.enableDeletionVectors' = 'false', 'delta.checkpointInterval' = '1000',
      'delta.checkpointPolicy' = 'classic')""")
    sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (-1, 'old'), (2, 'old'), (2, 'old'), (CAST(NULL AS INT), 'old') AS v(id, value)""")
    sql("""INSERT OVERWRITE tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (7, 'keep'), (3, CAST(NULL AS STRING)), (3, CAST(NULL AS STRING)),
      (CAST(NULL AS INT), 'missing') AS v(id, value)""")
    sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (9, 'boundary'), (7, 'keep') AS v(id, value)""")
    val table = registerTable("tbl")
    val schema = new StructType().add("id", IntegerType).add("value", StringType)
    val boundaryRows = Seq(Row(7, "keep"), Row(3, null), Row(3, null), Row(null, "missing"),
      Row(9, "boundary"), Row(7, "keep"))
    val latestRows = boundaryRows ++ Seq(Row(3, null), Row(11, "after"), Row(null, "after"))
    val original = (0L to 3L).map(v => v -> CommitLog.read(table.sourcePath, v)).toMap
    val metadata = original(0L).collect { case m: Metadata => m }.ensuring(_.size == 1).head
    assert(original(0L).collect { case p: Protocol => p } == Seq(Protocol(1, 2)))
    assert(DataType.fromJson(metadata.schemaString) == schema)
    assert(metadata.format == io.delta.workload.log.Format())
    assert(metadata.partitionColumns.isEmpty)
    assert(metadata.configuration == Map("delta.enableDeletionVectors" -> "false",
      "delta.checkpointInterval" -> "1000", "delta.checkpointPolicy" -> "classic"))
    val inputs = (1L to 3L).map { version =>
      val adds = original(version).collect { case a: AddFile => a }
      assert(adds.size == 1 && adds.head.deletionVector.isEmpty)
      version -> adds.head
    }.toMap
    assert(inputs.values.map(_.path).toSet.size == 3)
    assert(original(2L).collect { case r: RemoveFile => r.path } == Seq(inputs(1L).path))
    DeltaHarness.get.openLog(spark, table.sourcePath.toString).checkpoint()
    val checkpointName = "00000000000000000003.checkpoint.parquet"
    val checkpointBytes = Files.readAllBytes(table.sourcePath.resolve("_delta_log")
      .resolve(checkpointName))
    var copiedTable: Path = null
    mutateTable(table) { dir =>
      copiedTable = dir
      val logDir = dir.resolve("_delta_log")
      (0L to 3L).foreach(v => assert(CommitLog.read(dir, v) == original(v)))
      assert(Files.readAllBytes(logDir.resolve(checkpointName)).sameElements(checkpointBytes))
      // Retain v2 and v3 JSON for OSS historical lookup, plus the oldest checkpoint at v3.
      // The missing prefix makes v1 unavailable; no table data is deleted.
      (0L to 1L).foreach { version =>
        val commit = logDir.resolve(f"$version%020d.json")
        assert(Files.isRegularFile(commit))
        Files.delete(commit)
        assert(!Files.exists(commit))
      }
      (2L to 3L).foreach(v => assert(CommitLog.read(dir, v) == original(v)))
      inputs.values.foreach(a => assert(Files.size(dir.resolve(a.path)) == a.size))
      val boundary = SnapshotResolver.buildDeltaReader(spark, dir, Some(3L), None)
      assert(boundary.schema == schema)
      RowComparison.assertRowsEqual(spark.createDataFrame(boundaryRows.asJava, schema), boundary,
        "history boundary after JSON removal, before append")
      // Append to the already truncated copy: v4 must be replayed after the v3 checkpoint.
      spark.sql(s"""INSERT INTO delta.`$dir` SELECT /*+ COALESCE(1) */ * FROM VALUES
        (3, CAST(NULL AS STRING)), (11, 'after'), (CAST(NULL AS INT), 'after') AS v(id, value)""")
        .collect()
      // The generator strips Hadoop checksum sidecars while copying. The append's LogStore
      // may recreate this sidecar; keep the same portable fixture form after the mutation.
      Files.deleteIfExists(logDir.resolve(".00000000000000000004.json.crc"))
      val appended = CommitLog.read(dir, 4L)
      val adds = appended.collect { case a: AddFile => a }
      assert(adds.size == 1 && adds.head.deletionVector.isEmpty)
      assert(!inputs.values.exists(_.path == adds.head.path))
      assert(appended.forall(a => !a.isInstanceOf[RemoveFile] && !a.isInstanceOf[Metadata] &&
        !a.isInstanceOf[Protocol]))
      val commitInfo = appended.map(a => JsonUtil.mapper.readTree(a.toJson))
        .filter(_.has("commitInfo")).ensuring(_.size == 1).head.get("commitInfo")
      assert(commitInfo.get("operation").asText() == "WRITE")
      assert(commitInfo.get("operationParameters").get("mode").asText() == "Append")
      assert(Files.size(dir.resolve(adds.head.path)) == adds.head.size)
      assert(Files.readAllBytes(logDir.resolve(checkpointName)).sameElements(checkpointBytes))
      val files = Files.list(logDir)
      try {
        val names = files.iterator().asScala.map(_.getFileName.toString).toSeq
        assert(names.filter(_.endsWith(".checkpoint.parquet")) == Seq(checkpointName))
        assert(names.filter(_.endsWith(".json")).sorted == (2L to 4L).map(v => f"$v%020d.json"))
      } finally files.close()
      assert(JsonUtil.mapper.readTree(Files.readAllBytes(logDir.resolve("_last_checkpoint")))
        .get("version").asLong() == 3L)
      (2L to 3L).foreach(v => assert(CommitLog.read(dir, v) == original(v)))
      inputs.values.foreach(a => assert(Files.size(dir.resolve(a.path)) == a.size))
      info("history removed JSON v0/v1; retained v2/v3 and checkpoint v3; real append v4: " +
        commitInfo.toString)
    }
    val expectedSnapshot = SnapshotResult(ProtocolInfo(1, 2),
      MetadataInfo(metadata.id, metadata.name, metadata.description,
        HarnessFormat("parquet", Map.empty), schema.json, Seq.empty,
        metadata.configuration, metadata.createdTime))
    Seq[java.lang.Long](3L, null).foreach { version =>
      snapshotSpec(table, version = version).assert { spec =>
        assert(spec.query == SnapshotQuery(version = Some(Option(version).fold(4L)(_.toLong))))
        assert(spec.expectation == Succeeded(expectedSnapshot))
      }
    }
    val cases = Seq(
      ("read_boundary_v3", ReadQuery(version = Some(3L)), boundaryRows, 2),
      ("read_latest_after_append", ReadQuery(), latestRows, 3))
    cases.foreach { case (name, query, rows, fileCount) =>
      readSpec(table, name = Some(name), version = query.version.map(Long.box).orNull)
        .assert { spec =>
          assert(spec.query == query)
          assert(spec.expectation == Succeeded(ReadResult(rows.size.toLong, fileCount, 0L)))
          val expected = spark.createDataFrame(rows.asJava, schema)
          val actual = SnapshotResolver.buildDeltaReader(spark, copiedTable, query.version, None)
          assert(actual.schema == schema)
          RowComparison.assertRowsEqual(expected, actual, s"history $name copied table")
          val fixture = copiedTable.getParent
          val specName = s"${fixture.getFileName}_$name"
          val captured = spark.read.parquet(
            fixture.resolve(s"expected/$specName/expected_data").toString)
          assert(captured.schema == schema)
          RowComparison.assertRowsEqual(expected, captured, s"history $name expected parquet")
        }
    }
    // No checkpoint or JSON exists at or before v1. DAT resolves getSnapshotAt first;
    // OSS raises DELTA_LOG_FILE_NOT_FOUND for that empty log segment, before scanning data.
    readSpec(table, name = Some("read_unavailable_v1"), version = 1L,
      expectError = ErrorCode("DELTA_LOG_FILE_NOT_FOUND")).assert { spec =>
      assert(spec.query == ReadQuery(version = Some(1L)))
      spec.expectation match {
        case Failed(error) =>
          assert(error.errorCode == "DELTA_LOG_FILE_NOT_FOUND")
          assert(error.errorMessage.contains("version 1"))
          info("history typed unavailable-history read: " + error.toString)
        case other => fail(s"history unavailable-history read expected Failed, got $other")
      }
    }
    snapshotSpec(table, version = 1L, expectError = ErrorCode("DELTA_LOG_FILE_NOT_FOUND"))
      .assert { spec =>
        assert(spec.query == SnapshotQuery(version = Some(1L)))
        spec.expectation match {
          case Failed(error) =>
            assert(error.errorCode == "DELTA_LOG_FILE_NOT_FOUND")
            assert(error.errorMessage.contains("version 1"))
          case other => fail(s"history unavailable-history snapshot expected Failed, got $other")
        }
      }
  }

  test("e2e_50_actual_vacuum") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta TBLPROPERTIES (
      'delta.enableDeletionVectors' = 'false', 'delta.checkpointInterval' = '1000',
      'delta.checkpointPolicy' = 'classic')""")
    sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (8, 'keep'), (3, CAST(NULL AS STRING)), (3, CAST(NULL AS STRING)),
      (CAST(NULL AS INT), 'missing') AS v(id, value)""")
    sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (-2, 'early'), (5, 'middle'), (8, 'keep'), (12, CAST(NULL AS STRING)) AS v(id, value)""")
    val table = registerTable("tbl")
    val schema = new StructType().add("id", IntegerType).add("value", StringType)
    val rows = Seq(Row(8, "keep"), Row(3, null), Row(3, null), Row(null, "missing"),
      Row(-2, "early"), Row(5, "middle"), Row(8, "keep"), Row(12, null))
    val literal = spark.createDataFrame(rows.asJava, schema)
    val before = (0L to 2L).map(v => v -> CommitLog.read(table.sourcePath, v)).toMap
    val metadata = before(0L).collect { case m: Metadata => m }.ensuring(_.size == 1).head
    assert(before(0L).collect { case p: Protocol => p } == Seq(Protocol(1, 2)))
    assert(DataType.fromJson(metadata.schemaString) == schema)
    assert(metadata.format == io.delta.workload.log.Format())
    assert(metadata.partitionColumns.isEmpty)
    assert(metadata.configuration == Map("delta.enableDeletionVectors" -> "false",
      "delta.checkpointInterval" -> "1000", "delta.checkpointPolicy" -> "classic"))
    val inputs = (1L to 2L).flatMap(v => before(v).collect { case a: AddFile => a })
    assert(inputs.size == 2 && inputs.map(_.path).distinct.size == 2)
    assert(inputs.forall(_.deletionVector.isEmpty))
    RowComparison.assertRowsEqual(literal,
      SnapshotResolver.buildDeltaReader(spark, table.sourcePath, Some(2L), None),
      "vacuum historical rows before pruning")
    sql("OPTIMIZE tbl")
    val rewrite = CommitLog.read(table.sourcePath, 3L)
    val active = rewrite.collect { case a: AddFile => a }
    val obsolete = rewrite.collect { case r: RemoveFile => r }
    assert(active.nonEmpty && obsolete.size == 2)
    assert(obsolete.map(_.path).toSet == inputs.map(_.path).toSet)
    assert((active.map(_.path).toSet intersect obsolete.map(_.path).toSet).isEmpty)
    assert(active.forall(a => !a.dataChange && a.deletionVector.isEmpty))
    assert(obsolete.forall(r => !r.dataChange && r.deletionVector.isEmpty))
    assert(rewrite.collect { case m: Metadata => m }.isEmpty)
    assert(rewrite.collect { case p: Protocol => p }.isEmpty)
    val optimizeInfo = rewrite.map(a => JsonUtil.mapper.readTree(a.toJson))
      .filter(_.has("commitInfo")).ensuring(_.size == 1).head.get("commitInfo")
    assert(optimizeInfo.get("operation").asText() == "OPTIMIZE")
    (inputs ++ active).foreach { add =>
      assert(Files.isRegularFile(table.sourcePath.resolve(add.path)))
      assert(Files.size(table.sourcePath.resolve(add.path)) == add.size)
    }
    info("vacuum pre-VACUUM obsolete paths physically present: " +
      obsolete.map(_.path).mkString(","))
    val retentionCheck = "spark.databricks.delta.retentionDurationCheck.enabled"
    val originalRetentionCheck = spark.conf.getOption(retentionCheck)
    try {
      spark.conf.set(retentionCheck, "false")
      sql("VACUUM tbl RETAIN 0 HOURS")
    } finally {
      originalRetentionCheck match {
        case Some(value) => spark.conf.set(retentionCheck, value)
        case None => spark.conf.unset(retentionCheck)
      }
    }
    assert(spark.conf.getOption(retentionCheck) == originalRetentionCheck)
    obsolete.foreach(r => assert(!Files.exists(table.sourcePath.resolve(r.path))))
    active.foreach(a => assert(Files.size(table.sourcePath.resolve(a.path)) == a.size))
    // VACUUM emits audit commits; they must not change the active file set or metadata.
    val vacuum = (4L to 5L).map(v => v -> CommitLog.read(table.sourcePath, v)).toMap
    val vacuumInfo = vacuum.toSeq.sortBy(_._1).flatMap { case (_, actions) =>
      assert(actions.forall(a => !a.isInstanceOf[AddFile] && !a.isInstanceOf[RemoveFile] &&
        !a.isInstanceOf[Metadata] && !a.isInstanceOf[Protocol]))
      actions.map(a => JsonUtil.mapper.readTree(a.toJson)).filter(_.has("commitInfo"))
        .map(_.get("commitInfo"))
    }
    assert(vacuumInfo.map(_.get("operation").asText()) == Seq("VACUUM START", "VACUUM END"))
    vacuumInfo.foreach(n => info("vacuum actual vacuum commit: " + n.toString))
    var copiedTable: Path = null
    mutateTable(table) { dir =>
      copiedTable = dir
      (before ++ Map(3L -> rewrite) ++ vacuum).foreach { case (version, actions) =>
        assert(CommitLog.read(dir, version) == actions)
      }
      obsolete.foreach(r => assert(!Files.exists(dir.resolve(r.path))))
      active.foreach(a => assert(Files.size(dir.resolve(a.path)) == a.size))
      // Force row materialization, independently of DAT's typed error capture.
      val thrown = intercept[Exception] {
        SnapshotResolver.buildDeltaReader(spark, dir, Some(2L), None).collect()
      }
      val causes = Iterator.iterate(thrown: Throwable)(_.getCause).takeWhile(_ != null).toSeq
      assert(causes.exists(_.isInstanceOf[java.io.FileNotFoundException]))
      assert(causes.exists(e => obsolete.exists(r =>
        Option(e.getMessage).exists(_.contains(r.path)))))
      info("vacuum independent historical missing-file cause: " +
        causes.map(e => e.getClass.getName + ": " + e.getMessage).mkString(" <- "))
    }
    val expectedSnapshot = SnapshotResult(ProtocolInfo(1, 2),
      MetadataInfo(metadata.id, metadata.name, metadata.description,
        HarnessFormat("parquet", Map.empty), schema.json, Seq.empty,
        metadata.configuration, metadata.createdTime))
    Seq[java.lang.Long](3L, 5L, null).foreach { version =>
      snapshotSpec(table, version = version).assert { spec =>
        assert(spec.query == SnapshotQuery(version = Some(Option(version).fold(5L)(_.toLong))))
        assert(spec.expectation == Succeeded(expectedSnapshot))
      }
    }
    val projected = new StructType().add("value", StringType).add("id", IntegerType)
    val cases = Seq(
      ("read_latest", ReadQuery(), schema, rows),
      ("read_rewritten_v3", ReadQuery(version = Some(3L)), schema, rows),
      ("read_vacuumed_v5", ReadQuery(version = Some(5L)), schema, rows),
      ("read_latest_filtered", ReadQuery(predicate = Some("id = 3 OR id IS NULL"),
        columns = Some(Seq("value", "id"))), projected,
        Seq(Row(null, 3), Row(null, 3), Row("missing", null))))
    cases.foreach { case (name, query, expectedSchema, expectedRows) =>
      readSpec(table, name = Some(name), version = query.version.map(Long.box).orNull,
        predicate = query.predicate.orNull, columns = query.columns).assert { spec =>
        assert(spec.query == query)
        spec.expectation match {
          case Succeeded(result) => assert(result.rowCount == expectedRows.size.toLong)
          case other => fail(s"vacuum $name expected Succeeded, got $other")
        }
        val expected = spark.createDataFrame(expectedRows.asJava, expectedSchema)
        val actual = SnapshotResolver.applyFilters(
          SnapshotResolver.buildDeltaReader(spark, copiedTable, query.version, query.timestamp),
          query.predicate, query.columns)
        assert(actual.schema == expectedSchema)
        RowComparison.assertRowsEqual(expected, actual, s"vacuum $name copied table")
        val fixture = outputDir.resolve(WorkloadContext.current.workloadName)
        val specName = s"${fixture.getFileName}_$name"
        val captured = spark.read.parquet(
          fixture.resolve(s"expected/$specName/expected_data").toString)
        assert(captured.schema == expectedSchema)
        RowComparison.assertRowsEqual(expected, captured, s"vacuum $name expected parquet")
      }
    }
    readSpec(table, name = Some("read_pruned_v2"), version = 2L,
      expectError = AnyError).assert { spec =>
      assert(spec.query == ReadQuery(version = Some(2L)))
      spec.expectation match {
        case Failed(error) =>
          assert(error.errorCode.nonEmpty)
          assert(obsolete.exists(r => error.errorMessage.contains(r.path)))
          val message = error.errorMessage.toLowerCase(java.util.Locale.ROOT)
          assert(message.contains("not exist") || message.contains("not found"))
          info("vacuum typed historical error: " + error.toString)
        case other => fail(s"vacuum pruned historical read expected Failed, got $other")
      }
      // An error at the historical version must leave the current table readable.
      RowComparison.assertRowsEqual(literal,
        SnapshotResolver.buildDeltaReader(spark, copiedTable, None, None),
        "vacuum current rows after historical failure")
    }
  }

  test("e2e_55_restore") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta TBLPROPERTIES (
      'delta.enableDeletionVectors' = 'false', 'delta.checkpointInterval' = '1000',
      'delta.checkpointPolicy' = 'classic')""")
    sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (-7, 'before'), (4, CAST(NULL AS STRING)), (4, CAST(NULL AS STRING)),
      (CAST(NULL AS INT), 'missing'), (12, 'keep'), (12, 'keep') AS v(id, value)""")
    val table = registerTable("tbl")
    val before = (0L to 1L).map(v => v -> CommitLog.read(table.sourcePath, v)).toMap
    sql("""INSERT OVERWRITE tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (99, 'mutated'), (99, 'mutated'), (CAST(NULL AS INT), 'changed'),
      (6, CAST(NULL AS STRING)) AS v(id, value)""")
    val mutated = CommitLog.read(table.sourcePath, 2L)
    sql("RESTORE TABLE tbl TO VERSION AS OF 1")
    val restored = CommitLog.read(table.sourcePath, 3L)
    assert(!Files.exists(CommitLog.commitFile(table.sourcePath, 4L)))
    (0L to 1L).foreach(v => assert(CommitLog.read(table.sourcePath, v) == before(v)))
    assert(CommitLog.read(table.sourcePath, 2L) == mutated)
    val originalAdds = before(1L).collect { case a: AddFile => a }
    val mutatedAdds = mutated.collect { case a: AddFile => a }
    val restoredAdds = restored.collect { case a: AddFile => a }
    assert(originalAdds.nonEmpty && mutatedAdds.nonEmpty)
    assert(originalAdds.map(_.path).toSet.intersect(mutatedAdds.map(_.path).toSet).isEmpty)
    assert(mutated.collect { case r: RemoveFile => r.path }.toSet ==
      originalAdds.map(_.path).toSet)
    assert(restoredAdds.toSet == originalAdds.map(_.copy(dataChange = true)).toSet)
    val restoredRemoves = restored.collect { case r: RemoveFile => r }
    assert(restoredRemoves.map(_.path).toSet == mutatedAdds.map(_.path).toSet)
    assert(restoredAdds.forall(_.dataChange) && restoredRemoves.forall(_.dataChange))
    val commitInfo = restored.map(a => JsonUtil.mapper.readTree(a.toJson))
      .filter(_.has("commitInfo")).ensuring(_.size == 1).head.get("commitInfo")
    assert(commitInfo.get("operation").asText() == "RESTORE")
    assert(commitInfo.get("operationParameters").get("version").asText() == "1")
    info("restore OSS Delta 4.1.0 RESTORE commit: " + commitInfo.toString)

    val schema = new StructType().add("id", IntegerType).add("value", StringType)
    // Literal bags are independent of both the captured result and the Delta reader.
    val initialRows = Seq(Row(-7, "before"), Row(4, null), Row(4, null),
      Row(null, "missing"), Row(12, "keep"), Row(12, "keep"))
    val mutatedRows = Seq(Row(99, "mutated"), Row(99, "mutated"),
      Row(null, "changed"), Row(6, null))
    val metadata = before(0L).collect { case m: Metadata => m }.ensuring(_.size == 1).head
    val configuration = Map("delta.enableDeletionVectors" -> "false",
      "delta.checkpointInterval" -> "1000", "delta.checkpointPolicy" -> "classic")
    assert(before(0L).collect { case p: Protocol => p } == Seq(Protocol(1, 2)))
    assert(DataType.fromJson(metadata.schemaString) == schema)
    assert(metadata.partitionColumns.isEmpty && metadata.configuration == configuration)
    val expectedSnapshot = SnapshotResult(ProtocolInfo(1, 2),
      MetadataInfo(metadata.id, metadata.name, metadata.description,
        HarnessFormat("parquet", Map.empty), schema.json, Seq.empty,
        configuration, metadata.createdTime))
    var copiedTable: Path = null
    mutateTable(table) { dir =>
      copiedTable = dir
      assert(dir.getParent.getParent == outputDir)
      (0L to 1L).foreach(v => assert(CommitLog.read(dir, v) == before(v)))
      assert(CommitLog.read(dir, 2L) == mutated)
      assert(CommitLog.read(dir, 3L) == restored)
      (originalAdds ++ mutatedAdds).foreach { add =>
        assert(Files.readAllBytes(dir.resolve(add.path))
          .sameElements(Files.readAllBytes(table.sourcePath.resolve(add.path))))
      }
    }
    Seq[java.lang.Long](1L, 2L, null).foreach { version =>
      snapshotSpec(table, version = version).assert { spec =>
        assert(spec.query == SnapshotQuery(version = Some(Option(version).fold(3L)(_.toLong))))
        assert(spec.expectation == Succeeded(expectedSnapshot))
      }
    }
    val projected = new StructType().add("value", StringType).add("id", IntegerType)
    val cases = Seq(
      ("read_before_v1", ReadQuery(version = Some(1L)), schema, initialRows),
      ("read_mutated_v2", ReadQuery(version = Some(2L)), schema, mutatedRows),
      ("read_restored_v3", ReadQuery(version = Some(3L)), schema, initialRows),
      ("read_latest", ReadQuery(), schema, initialRows),
      ("read_latest_selective", ReadQuery(predicate = Some("id = 4 OR id IS NULL"),
        columns = Some(Seq("value", "id"))), projected,
        Seq(Row(null, 4), Row(null, 4), Row("missing", null))))
    cases.foreach { case (name, query, expectedSchema, rows) =>
      readSpec(table, name = Some(name), version = query.version.map(Long.box).orNull,
        predicate = query.predicate.orNull, columns = query.columns).assert { spec =>
        assert(spec.query == query)
        spec.expectation match {
          case Succeeded(result) => assert(result.rowCount == rows.size.toLong)
          case other => fail(s"restore $name expected Succeeded, got $other")
        }
        val expected = spark.createDataFrame(rows.asJava, expectedSchema)
        val actual = SnapshotResolver.applyFilters(
          SnapshotResolver.buildDeltaReader(spark, copiedTable, query.version, query.timestamp),
          query.predicate, query.columns)
        assert(actual.schema == expectedSchema)
        RowComparison.assertRowsEqual(expected, actual, s"restore $name copied table")
        val fixture = copiedTable.getParent
        val specName = s"${fixture.getFileName}_$name"
        val captured = spark.read.parquet(
          fixture.resolve(s"expected/$specName/expected_data").toString)
        assert(captured.schema == expectedSchema)
        RowComparison.assertRowsEqual(expected, captured, s"restore $name expected parquet")
      }
    }
  }

}
