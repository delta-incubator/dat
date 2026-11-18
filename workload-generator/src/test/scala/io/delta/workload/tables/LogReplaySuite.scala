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
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}

import io.delta.workload.WorkloadTestSuite
import io.delta.workload.deltaharness.{Format => HarnessFormat}
import io.delta.workload.engine.{RowComparison, SnapshotResolver}
import io.delta.workload.json.JsonUtil
import io.delta.workload.log.{Action, AddFile, CommitLog, Metadata, Protocol, RemoveFile}
import io.delta.workload.model._

/**
 * Log Replay + Production Edge Cases workloads (lr_* + log_* + lc_* + prod_*).
 * Covers: add-then-remove log replay, checkpoint supersedes log, full replay without
 * checkpoint, metadata latest wins, dataChange=false from compaction, missing
 * protocol/metadata errors, add-remove-readd, DV key dedup, last checkpoint info,
 * production edge cases (empty table, many commits, version gaps, truncated logs,
 * unknown reader features, varchar metadata, duplicate file refs, external checkpoints),
 * minor log compaction and automatic compaction.
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
    readSpec(t, predicate = "id = 2", name = Some("read_deleted"))
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

  // Minor log compaction and automatic compaction.
  test("e2e_56_minor_log_compaction") {
    import org.apache.spark.sql.delta.actions.{Action => SparkAction, InMemoryLogReplay}
    import org.apache.spark.sql.delta.util.FileNames

    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta TBLPROPERTIES (
      'delta.enableDeletionVectors' = 'false', 'delta.checkpointInterval' = '1000',
      'delta.checkpointPolicy' = 'classic')""")
    sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (-7, 'initial'), (4, CAST(NULL AS STRING)), (4, CAST(NULL AS STRING)),
      (CAST(NULL AS INT), 'missing'), (12, 'keep'), (12, 'keep') AS v(id, value)""")
    sql("""INSERT OVERWRITE tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (6, 'replacement'), (6, 'replacement'), (CAST(NULL AS INT), 'changed'),
      (8, CAST(NULL AS STRING)) AS v(id, value)""")
    sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (4, CAST(NULL AS STRING)), (4, CAST(NULL AS STRING)), (23, 'end') AS v(id, value)""")
    sql("""INSERT OVERWRITE tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (31, 'tail'), (31, 'tail'), (CAST(NULL AS INT), 'tail-null'),
      (4, CAST(NULL AS STRING)), (4, CAST(NULL AS STRING)) AS v(id, value)""")
    sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (44, 'last'), (44, 'last') AS v(id, value)""")
    val table = registerTable("tbl")
    val original = (0L to 5L).map(v => v -> CommitLog.read(table.sourcePath, v)).toMap
    val metadata = original(0L).collect { case m: Metadata => m }.ensuring(_.size == 1).head
    val schema = new StructType().add("id", IntegerType).add("value", StringType)
    val configuration = Map("delta.enableDeletionVectors" -> "false",
      "delta.checkpointInterval" -> "1000", "delta.checkpointPolicy" -> "classic")
    assert(original(0L).collect { case p: Protocol => p } == Seq(Protocol(1, 2)))
    assert(DataType.fromJson(metadata.schemaString) == schema)
    assert(metadata.partitionColumns.isEmpty && metadata.configuration == configuration)
    val adds = (1L to 5L).map { v =>
      v -> original(v).collect { case a: AddFile => a }.ensuring(_.size == 1).head
    }.toMap
    assert(adds.values.map(_.path).toSet.size == 5)
    assert(original(2L).collect { case r: RemoveFile => r.path } == Seq(adds(1L).path))
    assert(original(4L).collect { case r: RemoveFile => r.path }.toSet ==
      Set(adds(2L).path, adds(3L).path))

    // Follow the pinned OSS DeltaLogMinorCompactionSuite: reconcile [0, 3], with
    // no retention pruning, then serialize the existing OSS checkpoint iterator.
    // Concatenating commits would retain cancelled adds and commitInfo, which is invalid.
    val replay = new InMemoryLogReplay(None, None)
    (0L to 3L).foreach { v =>
      val lines = Files.readAllLines(table.sourcePath.resolve(f"_delta_log/$v%020d.json"))
      replay.append(v, lines.asScala.iterator.map(SparkAction.fromJson))
    }
    val sourceLog = DeltaLog.forTable(spark, table.sourcePath.toString)
    val compactedPath = FileNames.compactedDeltaFile(sourceLog.logPath, 0L, 3L)
    val compactedName = "00000000000000000000.00000000000000000003.compacted.json"
    assert(compactedPath.getName == compactedName)
    sourceLog.store.write(compactedPath, replay.checkpoint.map(_.json),
      overwrite = false, sourceLog.newDeltaHadoopConf())
    val compactedBytes = Files.readAllBytes(table.sourcePath.resolve(s"_delta_log/$compactedName"))
    val compacted = Files.readAllLines(table.sourcePath.resolve(s"_delta_log/$compactedName"))
      .asScala.map(Action.parse).toSeq
    assert(compacted.size == 5)
    assert(compacted.collect { case p: Protocol => p } == Seq(Protocol(1, 2)))
    assert(compacted.collect { case m: Metadata => m } == Seq(metadata))
    // OSS Action.fromJson materializes absent optional tags as an empty map.
    // FileAction.tagsOrEmpty defines their equivalence; preserve every nonempty tag.
    val expectedAdds = Set(adds(2L), adds(3L)).map(a =>
      a.copy(dataChange = false, tags = a.tags.filter(_.nonEmpty)))
    assert(compacted.collect { case a: AddFile => a.copy(tags = a.tags.filter(_.nonEmpty)) }
      .toSet == expectedAdds)
    assert(compacted.collect { case r: RemoveFile => r.copy(tags = r.tags.filter(_.nonEmpty)) } ==
      original(2L).collect { case r: RemoveFile =>
        r.copy(dataChange = false, tags = r.tags.filter(_.nonEmpty)) })
    info("compact compacted [0,3] actions: " + compacted.map(_.toJson).mkString("\n"))

    var copiedTable: Path = null
    mutateTable(table) { dir =>
      copiedTable = dir
      assert(dir.getParent.getParent == outputDir)
      assert(Files.readAllBytes(dir.resolve(s"_delta_log/$compactedName"))
        .sameElements(compactedBytes))
      // Compactions are optional accelerators. OSS validates original commit continuity;
      // neither compaction nor protocol cleanup authorizes removing this interval.
      (0L to 5L).foreach { v =>
        assert(CommitLog.read(dir, v) == original(v))
        assert(Files.readAllBytes(dir.resolve(f"_delta_log/$v%020d.json"))
          .sameElements(Files.readAllBytes(table.sourcePath.resolve(f"_delta_log/$v%020d.json"))))
      }
      adds.values.foreach { add =>
        assert(Files.readAllBytes(dir.resolve(add.path))
          .sameElements(Files.readAllBytes(table.sourcePath.resolve(add.path))))
      }
      val listing = Files.list(dir.resolve("_delta_log"))
      try assert(!listing.iterator().asScala.exists(_.getFileName.toString.contains("checkpoint")))
      finally listing.close()
    }
    val expectedSnapshot = SnapshotResult(ProtocolInfo(1, 2),
      MetadataInfo(metadata.id, metadata.name, metadata.description,
        HarnessFormat("parquet", Map.empty), schema.json, Seq.empty,
        configuration, metadata.createdTime))
    Seq[java.lang.Long](0L, 1L, 2L, 3L, 4L, 5L, null).foreach { version =>
      val v = Option(version).fold(5L)(_.toLong)
      snapshotSpec(table, version = version).assert { spec =>
        assert(spec.query == SnapshotQuery(version = Some(v)))
        assert(spec.expectation == Succeeded(expectedSnapshot))
        val snapshot = DeltaLog.forTable(spark, copiedTable.toString).getSnapshotAt(v)
        val selected = snapshot.logSegment.deltas.map(_.getPath.getName)
        val expected = if (v < 3L) (0L to v).map(n => f"$n%020d.json")
          else Seq(compactedName) ++ (4L to v).map(n => f"$n%020d.json")
        assert(selected == expected)
        assert(snapshot.logSegment.checkpointProvider.version == -1L)
        info(s"compact v$v selected log files: " + selected.mkString(","))
      }
    }
    val initialRows = Seq(Row(-7, "initial"), Row(4, null), Row(4, null),
      Row(null, "missing"), Row(12, "keep"), Row(12, "keep"))
    val replacementRows = Seq(Row(6, "replacement"), Row(6, "replacement"),
      Row(null, "changed"), Row(8, null))
    val endRows = replacementRows ++ Seq(Row(4, null), Row(4, null), Row(23, "end"))
    val tailRows = Seq(Row(31, "tail"), Row(31, "tail"), Row(null, "tail-null"),
      Row(4, null), Row(4, null))
    val latestRows = tailRows ++ Seq(Row(44, "last"), Row(44, "last"))
    val projected = new StructType().add("value", StringType).add("id", IntegerType)
    val cases = Seq(
      ("read_start_v0", ReadQuery(version = Some(0L)), schema, Seq.empty[Row]),
      ("read_inside_v1", ReadQuery(version = Some(1L)), schema, initialRows),
      ("read_inside_v2", ReadQuery(version = Some(2L)), schema, replacementRows),
      ("read_end_v3", ReadQuery(version = Some(3L)), schema, endRows),
      ("read_tail_v4", ReadQuery(version = Some(4L)), schema, tailRows),
      ("read_tail_v5", ReadQuery(version = Some(5L)), schema, latestRows),
      ("read_latest", ReadQuery(), schema, latestRows),
      ("read_latest_selective", ReadQuery(predicate = Some("id = 4 OR id IS NULL"),
        columns = Some(Seq("value", "id"))), projected,
        Seq(Row(null, 4), Row(null, 4), Row("tail-null", null))))
    cases.foreach { case (name, query, expectedSchema, rows) =>
      readSpec(table, name = Some(name), version = query.version.map(Long.box).orNull,
        predicate = query.predicate.orNull, columns = query.columns).assert { spec =>
        assert(spec.query == query)
        spec.expectation match {
          case Succeeded(result) => assert(result.rowCount == rows.size.toLong)
          case other => fail(s"compact $name expected Succeeded, got $other")
        }
        val expected = spark.createDataFrame(rows.asJava, expectedSchema)
        val actual = SnapshotResolver.applyFilters(
          SnapshotResolver.buildDeltaReader(spark, copiedTable, query.version, query.timestamp),
          query.predicate, query.columns)
        assert(actual.schema == expectedSchema)
        RowComparison.assertRowsEqual(expected, actual, s"compact $name copied table")
        val fixture = copiedTable.getParent
        val specName = s"${fixture.getFileName}_$name"
        val captured = spark.read.parquet(
          fixture.resolve(s"expected/$specName/expected_data").toString)
        assert(captured.schema == expectedSchema)
        RowComparison.assertRowsEqual(expected, captured, s"compact $name expected parquet")
      }
    }
  }
  test("e2e_57_auto_compaction") {
    // These settings affect fixture generation only. Ordinary SQL inserts invoke
    // OSS AutoCompact's post-commit hook; no explicit compaction command is called.
    val settings = Map(
      "spark.databricks.delta.autoCompact.enabled" -> "true",
      "spark.databricks.delta.autoCompact.minNumFiles" -> "3",
      "spark.databricks.delta.autoCompact.minFileSize" -> "1048576",
      "spark.databricks.delta.autoCompact.maxFileSize" -> "2097152")
    val saved = settings.keys.map(k => k -> spark.conf.getOption(k)).toMap
    try {
      settings.foreach { case (key, value) => sql(s"SET $key=$value") }
      sql("""CREATE TABLE tbl (id INT, value STRING) USING delta TBLPROPERTIES (
        'delta.autoOptimize.autoCompact' = 'true', 'delta.enableDeletionVectors' = 'false',
        'delta.checkpointInterval' = '1000', 'delta.checkpointPolicy' = 'classic')""")
      sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
        (-7, 'initial'), (4, CAST(NULL AS STRING)), (4, CAST(NULL AS STRING)),
        (CAST(NULL AS INT), 'missing') AS v(id, value)""")
      sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
        (12, 'keep'), (12, 'keep'), (23, 'end') AS v(id, value)""")
      sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
        (44, 'last'), (44, 'last'), (CAST(NULL AS INT), CAST(NULL AS STRING)),
        (88, 'edge') AS v(id, value)""")
      sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
        (99, 'tail'), (99, 'tail') AS v(id, value)""")
    } finally {
      saved.foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
    val table = registerTable("tbl")
    val commits = (0L to 5L).map(v => v -> CommitLog.read(table.sourcePath, v)).toMap
    assert(!Files.exists(CommitLog.commitFile(table.sourcePath, 6L)))
    val inputAdds = (1L to 3L).flatMap { v =>
      val adds = commits(v).collect { case a: AddFile => a }
      assert(adds.size == 1 && adds.head.dataChange)
      assert(adds.head.size > 0L && adds.head.size < 1048576L)
      assert(commits(v).collect { case r: RemoveFile => r }.isEmpty)
      adds
    }
    val rewritten = commits(4L).collect { case a: AddFile => a }
    val removed = commits(4L).collect { case r: RemoveFile => r }
    assert(rewritten.size == 1 && !rewritten.head.dataChange)
    assert(removed.size == 3 && removed.forall(r => !r.dataChange))
    assert(removed.map(_.path).toSet == inputAdds.map(_.path).toSet)
    assert(!inputAdds.exists(_.path == rewritten.head.path))
    val tail = commits(5L).collect { case a: AddFile => a }
    assert(tail.size == 1 && tail.head.dataChange)
    assert(commits(5L).collect { case r: RemoveFile => r }.isEmpty)
    val history = (0L to 5L).map { v =>
      val info = commits(v).map(a => JsonUtil.mapper.readTree(a.toJson))
        .filter(_.has("commitInfo")).ensuring(_.size == 1).head.get("commitInfo")
      v -> info
    }.toMap
    assert(history(0L).get("operation").asText() == "CREATE TABLE")
    Seq(1L, 2L, 3L, 5L).foreach { v =>
      assert(history(v).get("operation").asText() == "WRITE")
    }
    assert(history(4L).get("operation").asText() == "OPTIMIZE")
    assert(history(4L).get("operationParameters").get("auto").asText() == "true")
    info("auto automatic compaction commit v4: " + history(4L).toString)

    val schema = new StructType().add("id", IntegerType).add("value", StringType)
    val metadata = commits(0L).collect { case m: Metadata => m }.ensuring(_.size == 1).head
    val configuration = Map("delta.autoOptimize.autoCompact" -> "true",
      "delta.enableDeletionVectors" -> "false", "delta.checkpointInterval" -> "1000",
      "delta.checkpointPolicy" -> "classic")
    assert(commits(0L).collect { case p: Protocol => p } == Seq(Protocol(1, 2)))
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
      (0L to 5L).foreach { v =>
        assert(CommitLog.read(dir, v) == commits(v))
        assert(Files.readAllBytes(CommitLog.commitFile(dir, v))
          .sameElements(Files.readAllBytes(CommitLog.commitFile(table.sourcePath, v))))
      }
      (inputAdds ++ rewritten ++ tail).foreach { add =>
        assert(Files.readAllBytes(dir.resolve(add.path))
          .sameElements(Files.readAllBytes(table.sourcePath.resolve(add.path))))
      }
      val logs = Files.list(dir.resolve("_delta_log"))
      try assert(!logs.iterator().asScala.exists(_.getFileName.toString.contains("checkpoint")))
      finally logs.close()
    }
    val activePaths = Map(0L -> Set.empty[String], 1L -> Set(inputAdds(0).path),
      2L -> inputAdds.take(2).map(_.path).toSet, 3L -> inputAdds.map(_.path).toSet,
      4L -> Set(rewritten.head.path), 5L -> Set(rewritten.head.path, tail.head.path))
    Seq[java.lang.Long](0L, 1L, 2L, 3L, 4L, 5L, null).foreach { version =>
      val v = Option(version).fold(5L)(_.toLong)
      snapshotSpec(table, version = version).assert { spec =>
        assert(spec.query == SnapshotQuery(version = Some(v)))
        assert(spec.expectation == Succeeded(expectedSnapshot))
        val snapshot = DeltaLog.forTable(spark, copiedTable.toString).getSnapshotAt(v)
        assert(snapshot.allFiles.collect().map(_.path).toSet == activePaths(v))
        assert(snapshot.logSegment.checkpointProvider.version == -1L)
      }
    }
    // Literal, duplicate-sensitive bags are independent of captured expectations.
    val firstRows = Seq(Row(-7, "initial"), Row(4, null), Row(4, null), Row(null, "missing"))
    val secondRows = firstRows ++ Seq(Row(12, "keep"), Row(12, "keep"), Row(23, "end"))
    val beforeRows = secondRows ++ Seq(Row(44, "last"), Row(44, "last"),
      Row(null, null), Row(88, "edge"))
    val latestRows = beforeRows ++ Seq(Row(99, "tail"), Row(99, "tail"))
    val projected = new StructType().add("value", StringType).add("id", IntegerType)
    val cases = Seq(
      ("read_first_v1", ReadQuery(version = Some(1L)), schema, firstRows, 1, 0L),
      ("read_below_threshold_v2", ReadQuery(version = Some(2L)), schema, secondRows, 2, 0L),
      ("read_before_v3", ReadQuery(version = Some(3L)), schema, beforeRows, 3, 0L),
      ("read_post_v4", ReadQuery(version = Some(4L)), schema, beforeRows, 1, 0L),
      ("read_tail_v5", ReadQuery(version = Some(5L)), schema, latestRows, 2, 0L),
      ("read_latest", ReadQuery(), schema, latestRows, 2, 0L),
      ("read_latest_selective", ReadQuery(predicate = Some("id = 4 OR id IS NULL"),
        columns = Some(Seq("value", "id"))), projected,
        Seq(Row(null, 4), Row(null, 4), Row("missing", null), Row(null, null)), 1, 1L))
    cases.foreach { case (name, query, expectedSchema, rows, fileCount, skipped) =>
      readSpec(table, name = Some(name), version = query.version.map(Long.box).orNull,
        predicate = query.predicate.orNull, columns = query.columns).assert { spec =>
        assert(spec.query == query)
        assert(spec.expectation == Succeeded(ReadResult(rows.size.toLong, fileCount, skipped)))
        val expected = spark.createDataFrame(rows.asJava, expectedSchema)
        val actual = SnapshotResolver.applyFilters(
          SnapshotResolver.buildDeltaReader(spark, copiedTable, query.version, query.timestamp),
          query.predicate, query.columns)
        assert(actual.schema == expectedSchema)
        RowComparison.assertRowsEqual(expected, actual, s"auto $name copied table")
        val fixture = copiedTable.getParent
        val specName = s"${fixture.getFileName}_$name"
        val captured = spark.read.parquet(
          fixture.resolve(s"expected/$specName/expected_data").toString)
        assert(captured.schema == expectedSchema)
        RowComparison.assertRowsEqual(expected, captured, s"auto $name expected parquet")
      }
    }
  }

}
