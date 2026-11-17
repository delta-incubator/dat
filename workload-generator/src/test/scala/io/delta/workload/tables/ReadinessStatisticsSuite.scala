/*
 * Copyright (2026) The Delta Lake Project Authors.
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
import java.sql.Timestamp
import java.time.Instant

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DecimalType, IntegerType}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import io.delta.workload.WorkloadTestSuite
import io.delta.workload.engine.RowComparison
import io.delta.workload.json.JsonUtil
import io.delta.workload.log.{AddFile, CommitLog, Metadata, RemoveFile}
import io.delta.workload.model.Succeeded

/** Portable read specifications for incomplete or selected file statistics. */
class ReadinessStatisticsSuite extends WorkloadTestSuite("readiness_statistics") {
  test("missing_add_stats_predicates") {
    val schema = new StructType().add("id", IntegerType).add("value", IntegerType)
      .add("flag", BooleanType).add("items", ArrayType(IntegerType, containsNull = true))
    val nullArray = Row(1, 1, true, null)
    val emptyArray = Row(2, 2, false, Seq.empty[Integer])
    val nullElement = Row(3, null, null, Seq[Integer](null))
    val duplicate = Row(4, 3, true, Seq[Integer](1, 7))
    val nonmatching = Row(5, 4, false, Seq[Integer](2, 3))
    val matchingWithNull = Row(6, 5, null, Seq[Integer](7, null))
    val nonmatchingWithNull = Row(7, 2, true, Seq[Integer](null, 2))
    val matching = Row(8, null, false, Seq[Integer](7))
    val batches = Seq(Seq(nullArray, emptyArray, nullElement, duplicate, duplicate),
      Seq(nonmatching, matchingWithNull, nonmatchingWithNull, matching))
    val rows = batches.flatten
    val properties = Map("delta.enableDeletionVectors" -> "false",
      "delta.checkpointInterval" -> "100")
    sql("""CREATE TABLE tbl (id INT, value INT, flag BOOLEAN, items ARRAY<INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'false',
        'delta.checkpointInterval' = '100')""")
    sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (1, 1, TRUE, CAST(NULL AS ARRAY<INT>)), (2, 2, FALSE, CAST(array() AS ARRAY<INT>)),
      (3, NULL, NULL, array(CAST(NULL AS INT))), (4, 3, TRUE, array(1, 7)),
      (4, 3, TRUE, array(1, 7)) AS v(id, value, flag, items)""")
    sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (5, 4, FALSE, array(2, 3)), (6, 5, NULL, array(7, CAST(NULL AS INT))),
      (7, 2, TRUE, array(CAST(NULL AS INT), 2)), (8, NULL, FALSE, array(7))
      AS v(id, value, flag, items)""")
    val t = registerTable("tbl")
    var copiedTable: Path = null
    mutateTable(t) { dir =>
      copiedTable = dir
      val metadata = CommitLog.read(dir, 0).collect { case m: Metadata => m }.head
      require(DataType.fromJson(metadata.schemaString) == schema)
      require(metadata.configuration == properties && metadata.partitionColumns.isEmpty)
      batches.zipWithIndex.foreach { case (batch, index) =>
        val adds = CommitLog.read(dir, index.toLong + 1).collect { case a: AddFile => a }
        require(adds.size == 1 && adds.head.stats.isDefined)
        val add = adds.head
        RowComparison.assertRowsEqual(spark.createDataFrame(batch.asJava, schema),
          spark.read.parquet(dir.resolve(add.path).toString), s"predicate_input_$index")
        val stats = JsonUtil.mapper.readTree(add.stats.get)
        require(stats.path("numRecords").asLong(-1) == batch.size)
        // Arrays have no scalar min/max statistics; their predicates require residual filtering.
        require(!stats.path("minValues").has("items") && !stats.path("maxValues").has("items"))
      }
      val stream = Files.list(dir.resolve("_delta_log"))
      val names = try stream.iterator().asScala.map(_.getFileName.toString).toVector
        finally stream.close()
      require(!names.exists(_.contains("checkpoint")))
      // A cached allFiles set in a version CRC must not hide the missing AddFile statistics.
      names.filter(_.matches("[0-9]{20}\\.crc"))
        .foreach(name => Files.delete(dir.resolve("_delta_log").resolve(name)))
    }
    for (version <- 1L to 2L) {
      modifyCommitActions(t, version) { actions =>
        actions.map {
          case a: AddFile => a.copy(stats = None)
          case other => other
        }
      }
    }
    mutateTable(t) { dir =>
      val actions = (0L to 2L).flatMap(v => CommitLog.read(dir, v))
      val adds = actions.collect { case a: AddFile => a }
      require(adds.size == 2 && adds.map(_.path).distinct.size == 2)
      require(adds.forall(_.stats.isEmpty), "Every persisted AddFile must omit statistics")
      require(!actions.exists(_.isInstanceOf[RemoveFile]))
    }

    // Expected bags come from literal values and SQL three-valued logic, without a Delta scan.
    val casePredicate = "CASE WHEN value IN (1, 3) THEN TRUE " +
      "WHEN value = 2 THEN FALSE ELSE CAST(NULL AS BOOLEAN) END"
    val queries = Seq[(String, String, Seq[Row])](
      ("all", null, rows),
      ("ne", "value <> 2", Seq(nullArray, duplicate, duplicate, nonmatching, matchingWithNull)),
      ("in", "value IN (1, 3, 99)", Seq(nullArray, duplicate, duplicate)),
      ("in_with_null", "value IN (2, NULL)", Seq(emptyArray, nonmatchingWithNull)),
      ("in_miss", "value IN (99, NULL)", Seq.empty),
      ("is_not_true", "flag IS NOT TRUE",
        Seq(emptyArray, nullElement, nonmatching, matchingWithNull, matching)),
      ("array_contains", "array_contains(items, 7)",
        Seq(duplicate, duplicate, matchingWithNull, matching)),
      ("array_contains_miss", "array_contains(items, 99)", Seq.empty),
      ("array_contains_is_not_true", "array_contains(items, 7) IS NOT TRUE",
        Seq(nullArray, emptyArray, nullElement, nonmatching, nonmatchingWithNull)),
      ("array_contains_not", "NOT array_contains(items, 7)", Seq(emptyArray, nonmatching)),
      ("case_when", casePredicate, Seq(nullArray, duplicate, duplicate)),
      ("case_when_is_not_true", s"($casePredicate) IS NOT TRUE",
        Seq(emptyArray, nullElement, nonmatching, matchingWithNull, nonmatchingWithNull, matching)))
    queries.foreach { case (name, predicate, expectedRows) =>
      readSpec(t, predicate = predicate, name = Some(s"read_$name")).assert { spec =>
        spec.expectation match {
          case Succeeded(result) => require(result.rowCount == expectedRows.size)
          case other => fail(s"Expected successful missing-stat $name read, got $other")
        }
        val outputName = copiedTable.getParent.getFileName.toString
        val expectedPath = outputDir.resolve(outputName).resolve("expected")
          .resolve(s"${outputName}_read_$name").resolve("expected_data")
        RowComparison.assertRowsEqual(spark.createDataFrame(expectedRows.asJava, schema),
          spark.read.parquet(expectedPath.toString), s"independent_missing_stats_oracle_$name")
      }
    }
    snapshotSpec(t).assert { spec =>
      spec.expectation match {
        case Succeeded(result) =>
          require(DataType.fromJson(result.metadata.schemaString) == schema)
          require(result.metadata.configuration == properties)
          require(result.metadata.partitionColumns.isEmpty)
        case other => fail(s"Expected successful missing-stat snapshot, got $other")
      }
    }
  }

  for (variant <- Seq("no_null_count", "no_num_records", "neither_count", "partial_null_count")) {
    test(s"partial_counts_$variant") {
      val schema = new StructType().add("id", IntegerType).add("value", IntegerType)
      val batches = Seq(
        Seq(Row(1, 10), Row(2, null), Row(3, 30), Row(3, 30)),
        Seq(Row(4, 40), Row(5, 50)),
        Seq(Row(6, null), Row(7, null)),
        Seq(Row(8, 80), Row(9, null)))
      val rows = batches.flatten
      val properties = Map("delta.enableDeletionVectors" -> "false",
        "delta.checkpointInterval" -> "100")
      sql("""CREATE TABLE tbl (id INT, value INT) USING delta TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'false', 'delta.checkpointInterval' = '100')""")
      batches.foreach { batch =>
        // One mixed/all-null/non-null file per commit; duplicate rows must survive reads.
        val values = batch.map { r =>
          s"(${r.getInt(0)}, ${if (r.isNullAt(1)) "NULL" else r.getInt(1).toString})"
        }.mkString(", ")
        sql(s"INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES $values AS v(id, value)")
      }
      val t = registerTable("tbl")
      var copiedTable: Path = null

      // Check real AddFiles and their row-derived stats, then mutate only the copied table.
      mutateTable(t) { dir =>
        copiedTable = dir
        val initial = CommitLog.read(dir, 0)
        val originalMetadata = initial.collect { case m: Metadata => m }.head
        require(DataType.fromJson(originalMetadata.schemaString) == schema)
        require(originalMetadata.configuration == properties)
        require(originalMetadata.partitionColumns.isEmpty)
        val logFiles = Files.list(dir.resolve("_delta_log"))
        try {
          val names = logFiles.iterator().asScala.map(_.getFileName.toString).toSeq
          require(!names.exists(_.contains("checkpoint")), "Commit replay must be required")
          // Version CRCs can contain a cached allFiles set with the original stats.
          names.filter(_.matches("[0-9]{20}\\.crc")).foreach { name =>
            Files.delete(dir.resolve("_delta_log").resolve(name))
          }
        } finally logFiles.close()

        batches.zipWithIndex.foreach { case (batch, index) =>
          val version = index.toLong + 1
          val actions = CommitLog.read(dir, version)
          val adds = actions.collect { case a: AddFile => a }
          require(adds.size == 1, s"Expected one AddFile at version $version")
          val add = adds.head
          require(add.deletionVector.isEmpty && add.baseRowId.isEmpty &&
            add.defaultRowCommitVersion.isEmpty)
          val oracle = spark.createDataFrame(batch.asJava, schema)
          RowComparison.assertRowsEqual(oracle,
            spark.read.parquet(dir.resolve(add.path).toString), s"input_v$version")
          val stats = JsonUtil.mapper.readTree(add.stats.get).asInstanceOf[ObjectNode]
          require(stats.path("numRecords").asLong(-1) == batch.size)
          for ((column, ordinal) <- Seq("id" -> 0, "value" -> 1)) {
            val values = batch.filterNot(_.isNullAt(ordinal)).map(_.getInt(ordinal))
            require(stats.path("nullCount").path(column).asLong(-1) ==
              batch.count(_.isNullAt(ordinal)))
            if (values.nonEmpty) {
              require(stats.path("minValues").path(column).asInt(Int.MinValue) == values.min)
              require(stats.path("maxValues").path(column).asInt(Int.MinValue) == values.max)
            }
          }
          val before = stats.deepCopy()
          // Keep the last file intact as a control for per-file partial statistics.
          if (version < 4) {
            variant match {
              case "no_null_count" => stats.remove("nullCount")
              case "no_num_records" => stats.remove("numRecords")
              case "neither_count" =>
                stats.remove("nullCount")
                stats.remove("numRecords")
              case "partial_null_count" =>
                if (version == 3) stats.remove("nullCount")
                else stats.get("nullCount").asInstanceOf[ObjectNode]
                  .remove(if (version == 1) "value" else "id")
            }
            require(stats != before, s"Mutation must change stats at version $version")
          } else require(stats == before)
          require(stats.get("minValues") == before.get("minValues"))
          require(stats.get("maxValues") == before.get("maxValues"))
          val changed = add.copy(stats = Some(JsonUtil.mapper.writeValueAsString(stats)))
          CommitLog.write(dir, version, actions.map {
            case _: AddFile => changed
            case other => other
          })
          val reread = CommitLog.read(dir, version).collect { case a: AddFile => a }
          require(reread == Seq(changed), s"Persisted mutation mismatch at version $version")
          require(JsonUtil.mapper.readTree(reread.head.stats.get) == stats)
        }
      }

      // These bags are derived directly from the literal input, without a Delta scan or stats.
      val queries = Seq[(String, String, Seq[Row])](
        ("all", null, rows),
        ("value_is_null", "value IS NULL", rows.filter(_.isNullAt(1))),
        ("value_is_not_null", "value IS NOT NULL", rows.filterNot(_.isNullAt(1))),
        ("value_eq_30", "value = 30", Seq(Row(3, 30), Row(3, 30))),
        ("value_gt_35", "value > 35", Seq(Row(4, 40), Row(5, 50), Row(8, 80))),
        ("value_eq_999", "value = 999", Seq.empty),
        ("id_is_null", "id IS NULL", Seq.empty),
        ("id_is_not_null", "id IS NOT NULL", rows))
      queries.foreach { case (name, predicate, expectedRows) =>
        readSpec(t, predicate = predicate, name = Some(s"read_$name")).assert { spec =>
          spec.expectation match {
            case Succeeded(result) => require(result.rowCount == expectedRows.size)
            case other => fail(s"Expected successful $name read, got $other")
          }
          val outputName = copiedTable.getParent.getFileName.toString
          val expectedPath = outputDir.resolve(outputName).resolve("expected")
            .resolve(s"${outputName}_read_$name")
            .resolve("expected_data")
          RowComparison.assertRowsEqual(spark.createDataFrame(expectedRows.asJava, schema),
            spark.read.parquet(expectedPath.toString), s"independent_oracle_$name")
        }
      }
      snapshotSpec(t).assert { spec =>
        spec.expectation match {
          case Succeeded(result) =>
            require(DataType.fromJson(result.metadata.schemaString) == schema)
            require(result.metadata.configuration == properties)
            require(result.metadata.partitionColumns.isEmpty)
          case other => fail(s"Expected successful snapshot, got $other")
        }
      }
    }
  }

  test("remove_stats_without_active_add_stats") {
    val schema = new StructType().add("id", IntegerType).add("value", IntegerType)
    val batches = Seq(
      Seq(Row(1, 10), Row(5, null)),
      Seq(Row(2, 20), Row(3, 30), Row(3, 30), Row(4, null)),
      Seq(Row(100, 1000), Row(101, 1010)))
    val rewrittenRows = Seq(Row(3, 30), Row(3, 30), Row(4, null))
    val survivors = batches.head ++ rewrittenRows
    val properties = Map("delta.enableDeletionVectors" -> "false",
      "delta.checkpointInterval" -> "100")
    sql("""CREATE TABLE tbl (id INT, value INT) USING delta TBLPROPERTIES (
      'delta.enableDeletionVectors' = 'false', 'delta.checkpointInterval' = '100')""")
    batches.foreach { batch =>
      val values = batch.map { r =>
        s"(${r.getInt(0)}, ${if (r.isNullAt(1)) "NULL" else r.getInt(1).toString})"
      }.mkString(", ")
      sql(s"INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES $values AS v(id, value)")
    }
    // Rewrite one file, remove another entirely, and leave the first file untouched.
    sql("DELETE FROM tbl WHERE id = 2 OR id >= 100")
    val t = registerTable("tbl")
    var copiedTable: Path = null

    mutateTable(t) { dir =>
      copiedTable = dir
      val commits = (0L to 4L).map(v => v -> CommitLog.read(dir, v)).toMap
      val originalMetadata = commits(0).collect { case m: Metadata => m }.head
      require(DataType.fromJson(originalMetadata.schemaString) == schema)
      require(originalMetadata.configuration == properties)
      require(originalMetadata.partitionColumns.isEmpty)
      require(commits(0).collect { case a: AddFile => a }.isEmpty)
      val logFiles = Files.list(dir.resolve("_delta_log"))
      try {
        val names = logFiles.iterator().asScala.map(_.getFileName.toString).toSeq
        require(!names.exists(_.contains("checkpoint")), "Commit replay must be required")
        // Prevent a version CRC's cached allFiles from hiding the stats mutation.
        names.filter(_.matches("[0-9]{20}\\.crc")).foreach { name =>
          Files.delete(dir.resolve("_delta_log").resolve(name))
        }
      } finally logFiles.close()

      val inputAdds = batches.zipWithIndex.map { case (batch, index) =>
        val adds = commits(index.toLong + 1).collect { case a: AddFile => a }
        require(adds.size == 1, s"Expected one input AddFile for batch $index")
        require(!commits(index.toLong + 1).exists(_.isInstanceOf[RemoveFile]))
        val add = adds.head
        RowComparison.assertRowsEqual(spark.createDataFrame(batch.asJava, schema),
          spark.read.parquet(dir.resolve(add.path).toString), s"original_batch_$index")
        val stats = JsonUtil.mapper.readTree(add.stats.get)
        require(stats.path("numRecords").asLong(-1) == batch.size)
        for ((column, ordinal) <- Seq("id" -> 0, "value" -> 1)) {
          val values = batch.filterNot(_.isNullAt(ordinal)).map(_.getInt(ordinal))
          require(stats.path("nullCount").path(column).asLong(-1) ==
            batch.count(_.isNullAt(ordinal)))
          require(stats.path("minValues").path(column).asInt(Int.MinValue) == values.min)
          require(stats.path("maxValues").path(column).asInt(Int.MinValue) == values.max)
        }
        add
      }
      val allAdds = commits.values.toSeq.flatten.collect { case a: AddFile => a }
      val addsByPath = allAdds.map(a => a.path -> a).toMap
      require(addsByPath.size == allAdds.size, "Each path must have one original Add")
      require(allAdds.forall(a => a.stats.isDefined && a.partitionValues.isEmpty &&
        a.deletionVector.isEmpty && a.baseRowId.isEmpty && a.defaultRowCommitVersion.isEmpty))
      val removes = commits(4).collect { case r: RemoveFile => r }
      val removedPaths = removes.map(_.path).toSet
      require(removes.size == 2 && removedPaths == inputAdds.tail.map(_.path).toSet)
      removes.foreach { r =>
        val original = addsByPath(r.path)
        require(r.dataChange && r.deletionTimestamp.isDefined)
        require(r.extendedFileMetadata.contains(true))
        require(r.partitionValues.contains(original.partitionValues))
        require(r.size.contains(original.size) && r.deletionVector.isEmpty)
        require(r.baseRowId.isEmpty && r.defaultRowCommitVersion.isEmpty)
        require(r.stats.forall(s => JsonUtil.mapper.readTree(s) ==
          JsonUtil.mapper.readTree(original.stats.get)))
      }
      val rewritten = commits(4).collect { case a: AddFile => a }
      require(rewritten.nonEmpty)
      RowComparison.assertRowsEqual(spark.createDataFrame(rewrittenRows.asJava, schema),
        rewritten.map(a => spark.read.parquet(dir.resolve(a.path).toString)).reduce(_.union(_)),
        "rewritten_survivors")
      val activePaths = addsByPath.keySet -- removedPaths
      require(activePaths == (rewritten.map(_.path).toSet + inputAdds.head.path))

      for (version <- 1L to 4L) {
        val changed = commits(version).map {
          case a: AddFile if activePaths.contains(a.path) => a.copy(stats = None)
          // Some writers omit Remove stats. Supply the exact, row-checked original Add stats.
          case r: RemoveFile => r.copy(stats = addsByPath(r.path).stats)
          case other => other
        }
        CommitLog.write(dir, version, changed)
        require(CommitLog.read(dir, version) == changed,
          s"Persisted action mutation mismatch at version $version")
      }
      val persisted = (0L to 4L).flatMap(v => CommitLog.read(dir, v))
      val persistedAdds = persisted.collect { case a: AddFile => a }
      val persistedRemoves = persisted.collect { case r: RemoveFile => r }
      require(persistedAdds.filter(a => activePaths.contains(a.path)).forall(_.stats.isEmpty))
      require(persistedAdds.filter(a => removedPaths.contains(a.path)).forall { a =>
        a == addsByPath(a.path)
      })
      require(persistedRemoves.size == 2 && persistedRemoves.forall { r =>
        r.stats.isDefined && r.stats == addsByPath(r.path).stats
      })
    }

    // Literal row bags, independent of Delta replay and statistics; duplicates are intentional.
    val queries = Seq[(String, String, Seq[Row])](
      ("all", null, survivors),
      ("hit_untouched", "id = 1", Seq(Row(1, 10))),
      ("hit_rewritten", "id = 3", Seq(Row(3, 30), Row(3, 30))),
      ("range", "value >= 30", Seq(Row(3, 30), Row(3, 30))),
      ("null", "value IS NULL", Seq(Row(5, null), Row(4, null))),
      ("miss", "id = 999", Seq.empty),
      ("deleted_partial", "id = 2", Seq.empty),
      ("deleted_file", "id >= 100", Seq.empty),
      ("mixed", "id IN (2, 3, 100)", Seq(Row(3, 30), Row(3, 30))))
    queries.foreach { case (name, predicate, expectedRows) =>
      readSpec(t, predicate = predicate, name = Some(s"read_$name")).assert { spec =>
        spec.expectation match {
          case Succeeded(result) => require(result.rowCount == expectedRows.size)
          case other => fail(s"Expected successful $name read, got $other")
        }
        val outputName = copiedTable.getParent.getFileName.toString
        val expectedPath = outputDir.resolve(outputName).resolve("expected")
          .resolve(s"${outputName}_read_$name")
          .resolve("expected_data")
        RowComparison.assertRowsEqual(spark.createDataFrame(expectedRows.asJava, schema),
          spark.read.parquet(expectedPath.toString), s"independent_oracle_$name")
      }
    }
    snapshotSpec(t).assert { spec =>
      spec.expectation match {
        case Succeeded(result) =>
          require(DataType.fromJson(result.metadata.schemaString) == schema)
          require(result.metadata.configuration == properties)
          require(result.metadata.partitionColumns.isEmpty)
        case other => fail(s"Expected successful snapshot, got $other")
      }
    }
  }

  test("unicode_string_stats_predicates") {
    val schema = new StructType().add("id", IntegerType).add("s", StringType)
    val head = "a" * 31
    // Exact source rows from WriteStatsSuite.utf8_boundary_string_min_max; no NULL rows.
    // This independent read fixture avoids depending on another suite's generated output.
    val rows = Seq(Row(1, head + "étail"), Row(2, head + "中tail"), Row(3, head + "😀"))
    sql("CREATE TABLE tbl (id INT, s STRING) USING delta")
    rows.foreach { row =>
      // Separate files give the three Unicode boundaries distinct min/max statistics.
      sql(s"""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
        (${row.getInt(0)}, '${row.getString(1)}') AS v(id, s)""")
    }
    val t = registerTable("tbl")
    var copiedTable: Path = null
    mutateTable(t) { dir =>
      copiedTable = dir
      rows.zipWithIndex.foreach { case (row, index) =>
        val adds = CommitLog.read(dir, index.toLong + 1).collect { case a: AddFile => a }
        require(adds.size == 1)
        val add = adds.head
        RowComparison.assertRowsEqual(spark.createDataFrame(Seq(row).asJava, schema),
          spark.read.parquet(dir.resolve(add.path).toString), s"unicode_input_$index")
        val stats = JsonUtil.mapper.readTree(add.stats.get)
        require(stats.path("numRecords").asLong(-1) == 1)
        require(stats.path("nullCount").path("s").asLong(-1) == 0)
        val value = row.getString(1)
        val minimum = stats.path("minValues").path("s").asText()
        val maximum = stats.path("maxValues").path("s").asText()
        require(minimum.startsWith(head) && minimum.codePointAt(31) == value.codePointAt(31))
        require(UTF8String.fromString(minimum).compareTo(UTF8String.fromString(value)) <= 0)
        require(UTF8String.fromString(value).compareTo(UTF8String.fromString(maximum)) <= 0)
      }
    }

    // Literal bags use UTF-8 binary ordering: e-acute < CJK < supplementary emoji.
    val queries = Seq[(String, String, Seq[Row])](
      ("all", null, rows),
      ("equal_2byte", s"s = '${head}étail'", Seq(rows(0))),
      ("equal_3byte", s"s = '${head}中tail'", Seq(rows(1))),
      ("equal_4byte", s"s = '${head}😀'", Seq(rows(2))),
      ("range_2_to_3byte", s"s >= '${head}étail' AND s < '${head}中tail'", Seq(rows(0))),
      ("range_3_to_4byte", s"s >= '${head}中tail' AND s < '${head}😀'", Seq(rows(1))),
      ("range_from_4byte", s"s >= '${head}😀'", Seq(rows(2))),
      ("range_through_4byte", s"s > '${head}étail' AND s <= '${head}😀'", rows.tail),
      ("miss_same_prefix", s"s = '${head}étail_missing'", Seq.empty),
      ("miss_above_4byte", s"s > '${head}😀'", Seq.empty),
      ("is_null", "s IS NULL", Seq.empty),
      ("is_not_null", "s IS NOT NULL", rows))
    queries.foreach { case (name, predicate, expectedRows) =>
      readSpec(t, predicate = predicate, name = Some(s"read_$name")).assert { spec =>
        spec.expectation match {
          case Succeeded(result) => require(result.rowCount == expectedRows.size)
          case other => fail(s"Expected successful Unicode $name read, got $other")
        }
        val outputName = copiedTable.getParent.getFileName.toString
        val expectedPath = outputDir.resolve(outputName).resolve("expected")
          .resolve(s"${outputName}_read_$name").resolve("expected_data")
        RowComparison.assertRowsEqual(spark.createDataFrame(expectedRows.asJava, schema),
          spark.read.parquet(expectedPath.toString), s"independent_unicode_oracle_$name")
      }
    }
    snapshotSpec(t).assert { spec =>
      spec.expectation match {
        case Succeeded(result) =>
          require(DataType.fromJson(result.metadata.schemaString) == schema)
          require(result.metadata.partitionColumns.isEmpty)
          require(result.metadata.configuration.isEmpty)
        case other => fail(s"Expected successful Unicode snapshot, got $other")
      }
    }
  }

  for (bounds <- Seq("true", "false", "absent")) {
    test(s"tight_bounds_$bounds") {
      val schema = new StructType().add("id", IntegerType).add("all_null", IntegerType)
        .add("mixed_null", IntegerType).add("non_null", IntegerType)
      val physical = Seq(Row(1, null, 10, 10), Row(2, null, null, 20),
        Row(3, null, 30, 30), Row(3, null, 30, 30), Row(4, null, null, 40))
      val live = Seq(Row(2, null, null, 20), Row(3, null, 30, 30), Row(3, null, 30, 30))
      val control = physical.map(r => Row(r.getInt(0) + 100, r.get(1), r.get(2), r.get(3)))
      // Existing cases cover non-DV partial counts; explicit bounds need controls here.
      val batches = Seq.fill(3)(physical) ++
        (if (bounds == "absent") Seq.empty else Seq.fill(2)(control))
      val rows = Seq.fill(3)(live).flatten ++
        (if (bounds == "absent") Seq.empty else Seq.fill(2)(control).flatten)
      val properties = Map("delta.enableDeletionVectors" -> "true",
        "delta.checkpointInterval" -> "100")
      sql("""CREATE TABLE tbl (id INT, all_null INT, mixed_null INT, non_null INT)
        USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
          'delta.checkpointInterval' = '100')""")
      batches.foreach { batch =>
        val values = batch.map { row =>
          row.toSeq.map(v => if (v == null) "NULL" else v.toString).mkString("(", ", ", ")")
        }.mkString(", ")
        sql(s"INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES $values " +
          "AS v(id, all_null, mixed_null, non_null)")
      }
      sql("DELETE FROM tbl WHERE id = 1 OR id = 4")
      val t = registerTable("tbl")
      var copiedTable: Path = null
      mutateTable(t) { dir =>
        copiedTable = dir
        val lastVersion = batches.size.toLong + 1
        val commits = (0L to lastVersion).map(v => v -> CommitLog.read(dir, v)).toMap
        val metadata = commits(0).collect { case m: Metadata => m }.head
        require(DataType.fromJson(metadata.schemaString) == schema)
        require(metadata.configuration == properties && metadata.partitionColumns.isEmpty)
        val originals = batches.indices.map { index =>
          val adds = commits(index.toLong + 1).collect { case a: AddFile => a }
          require(adds.size == 1 && adds.head.deletionVector.isEmpty)
          val add = adds.head
          RowComparison.assertRowsEqual(spark.createDataFrame(batches(index).asJava, schema),
            spark.read.parquet(dir.resolve(add.path).toString), s"physical_$bounds/$index")
          add
        }
        val deleted = commits(lastVersion)
        val dvAdds = deleted.collect { case a: AddFile => a }
        val removes = deleted.collect { case r: RemoveFile => r }
        require(dvAdds.size == 3 && removes.size == 3)
        require(dvAdds.map(_.path).toSet == originals.take(3).map(_.path).toSet)
        require(removes.map(_.path).toSet == dvAdds.map(_.path).toSet)
        require(dvAdds.forall(a => a.deletionVector.exists(_.cardinality == 2)))
        RowComparison.assertRowsEqual(spark.createDataFrame(rows.asJava, schema),
          spark.read.format("delta").load(dir.toString), s"live_before_mutation_$bounds")

        val stream = Files.list(dir.resolve("_delta_log"))
        val files = try stream.iterator().asScala.toVector finally stream.close()
        require(!files.exists(_.getFileName.toString.contains("checkpoint")))
        files.filter(_.getFileName.toString.matches("[0-9]{20}\\.crc")).foreach(Files.delete)
        for (version <- 1L to lastVersion) {
          val changed = commits(version).map {
            case add: AddFile if add.deletionVector.isDefined ||
                originals.drop(3).exists(_.path == add.path) =>
              val index = originals.indexWhere(_.path == add.path)
              require(index >= 0 && add.partitionValues.isEmpty)
              val valid = if (index < 3) live else control
              val basis = if (bounds == "false") batches(index) else valid
              val stats = JsonUtil.mapper.createObjectNode()
              // DV numRecords is always physical, including when tight null counts are live.
              stats.put("numRecords", batches(index).size)
              if (bounds != "absent") stats.put("tightBounds", bounds == "true")
              val minimum = stats.putObject("minValues")
              val maximum = stats.putObject("maxValues")
              val nulls = stats.putObject("nullCount")
              schema.fieldNames.zipWithIndex.foreach { case (column, ordinal) =>
                val values = basis.filterNot(_.isNullAt(ordinal)).map(_.getInt(ordinal))
                if (values.nonEmpty) {
                  minimum.put(column, values.min)
                  maximum.put(column, values.max)
                }
                nulls.put(column, basis.count(_.isNullAt(ordinal)))
              }
              index match {
                case 1 => stats.remove("nullCount")
                case 2 =>
                  nulls.remove("all_null")
                  nulls.remove("mixed_null")
                case 3 => stats.remove("numRecords")
                case 4 =>
                  stats.remove("numRecords")
                  stats.remove("nullCount")
                case _ =>
              }
              require(stats.has("tightBounds") == (bounds != "absent"))
              require(stats.has("numRecords") == (index < 3))
              if (index < 3) require(stats.path("numRecords").asInt() == 5)
              add.copy(stats = Some(JsonUtil.mapper.writeValueAsString(stats)))
            case other => other
          }
          CommitLog.write(dir, version, changed)
          require(CommitLog.read(dir, version) == changed,
            s"Persisted bounds mutation mismatch at version $version")
        }
      }

      // SQL three-valued predicates applied to literal row bags, independent of file stats.
      val queries = Seq[(String, String, Seq[Row])](
        ("all", null, rows),
        ("all_null", "all_null IS NULL", rows),
        ("all_null_not_null", "all_null IS NOT NULL", Seq.empty),
        ("all_null_equal", "all_null = 10", Seq.empty),
        ("mixed_null", "mixed_null IS NULL", rows.filter(_.isNullAt(2))),
        ("mixed_not_null", "mixed_null IS NOT NULL", rows.filterNot(_.isNullAt(2))),
        ("mixed_equal", "mixed_null = 30", rows.filter(r => !r.isNullAt(2) &&
          r.getInt(2) == 30)),
        ("non_null_is_null", "non_null IS NULL", Seq.empty),
        ("non_null_not_null", "non_null IS NOT NULL", rows),
        ("surviving_duplicates", "id = 3", Seq.fill(3)(live.tail).flatten),
        ("deleted_lower_bound", "id = 1", Seq.empty),
        ("deleted_upper_bound", "id = 4", Seq.empty),
        ("deleted_mixed_value", "id < 100 AND mixed_null = 10", Seq.empty),
        ("surviving_range", "non_null >= 20 AND non_null <= 30",
          rows.filter(r => r.getInt(3) >= 20 && r.getInt(3) <= 30)),
        ("outside_range", "non_null > 40", Seq.empty))
      queries.foreach { case (name, predicate, expectedRows) =>
        readSpec(t, predicate = predicate, name = Some(s"read_$name")).assert { spec =>
          spec.expectation match {
            case Succeeded(result) => require(result.rowCount == expectedRows.size)
            case other => fail(s"Expected successful $bounds/$name read, got $other")
          }
          val outputName = copiedTable.getParent.getFileName.toString
          val expectedPath = outputDir.resolve(outputName).resolve("expected")
            .resolve(s"${outputName}_read_$name").resolve("expected_data")
          RowComparison.assertRowsEqual(spark.createDataFrame(expectedRows.asJava, schema),
            spark.read.parquet(expectedPath.toString), s"independent_oracle_$bounds/$name")
        }
      }
      snapshotSpec(t).assert { spec =>
        spec.expectation match {
          case Succeeded(result) =>
            require(DataType.fromJson(result.metadata.schemaString) == schema)
            require(result.metadata.configuration == properties)
            require(result.metadata.partitionColumns.isEmpty)
          case other => fail(s"Expected successful $bounds snapshot, got $other")
        }
      }
    }
  }

  test("indexed_prefix_beyond_32") {
    val columns = (0 until 36).map(i => f"c$i%02d")
    val schema = columns.foldLeft(new StructType())((s, c) => s.add(c, IntegerType))
    // The first 32 fields are a fixed prefix; c32/c33 are selected, c34/c35 are not.
    val prefix = 0 until 32
    val duplicate = Row.fromSeq(prefix ++ Seq(320, 330, 340, 350))
    val mixed = Row.fromSeq(prefix ++ Seq(321, 331, 341, 351))
    val nullLate = Row.fromSeq(prefix ++ Seq(null, null, null, null))
    val batches = Seq(Seq(duplicate, duplicate), Seq(mixed, nullLate))
    val rows = batches.flatten
    val properties = Map("delta.dataSkippingNumIndexedCols" -> "34",
      "delta.enableDeletionVectors" -> "false", "delta.checkpointInterval" -> "100")
    val ddl = columns.map(c => s"$c INT").mkString(", ")
    sql(s"""CREATE TABLE tbl ($ddl) USING delta TBLPROPERTIES (
      'delta.dataSkippingNumIndexedCols' = '34', 'delta.enableDeletionVectors' = 'false',
      'delta.checkpointInterval' = '100')""")
    batches.foreach { batch =>
      val values = batch.map { row =>
        row.toSeq.map(v => if (v == null) "NULL" else v.toString).mkString("(", ", ", ")")
      }.mkString(", ")
      sql(s"INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES $values " +
        s"AS v(${columns.mkString(", ")})")
    }
    val t = registerTable("tbl")
    var copiedTable: Path = null
    mutateTable(t) { dir =>
      copiedTable = dir
      val metadata = CommitLog.read(dir, 0).collect { case m: Metadata => m }.head
      require(DataType.fromJson(metadata.schemaString) == schema)
      require(metadata.configuration == properties && metadata.partitionColumns.isEmpty)
      batches.zipWithIndex.foreach { case (batch, index) =>
        val adds = CommitLog.read(dir, index.toLong + 1).collect { case a: AddFile => a }
        require(adds.size == 1 && adds.head.deletionVector.isEmpty)
        val add = adds.head
        RowComparison.assertRowsEqual(spark.createDataFrame(batch.asJava, schema),
          spark.read.parquet(dir.resolve(add.path).toString), s"wide_input_$index")
        // Verify the writer's physical AddFile stats, without assuming a reader's stats schema.
        val stats = JsonUtil.mapper.readTree(add.stats.get)
        require(stats.path("numRecords").asLong(-1) == batch.size)
        for (field <- Seq("minValues", "maxValues", "nullCount")) {
          require(stats.path(field).fieldNames().asScala.toSet == columns.take(34).toSet)
        }
        columns.take(34).zipWithIndex.foreach { case (column, ordinal) =>
          val values = batch.filterNot(_.isNullAt(ordinal)).map(_.getInt(ordinal))
          require(stats.path("nullCount").path(column).asLong(-1) ==
            batch.count(_.isNullAt(ordinal)))
          require(stats.path("minValues").path(column).asInt(Int.MinValue) == values.min)
          require(stats.path("maxValues").path(column).asInt(Int.MinValue) == values.max)
        }
      }
    }

    val queries = Seq[(String, String, Seq[Row])](
      ("all", null, rows),
      ("selected_c32_equal", "c32 = 320", Seq(duplicate, duplicate)),
      ("selected_c33_equal", "c33 = 330", Seq(duplicate, duplicate)),
      ("selected_c33_range", "c33 >= 330 AND c33 < 331", Seq(duplicate, duplicate)),
      ("selected_c33_null", "c33 IS NULL", Seq(nullLate)),
      ("selected_c33_miss", "c33 = 999", Seq.empty),
      ("unselected_c34_equal", "c34 = 340", Seq(duplicate, duplicate)),
      ("unselected_c34_range", "c34 > 340", Seq(mixed)),
      ("unselected_c34_null", "c34 IS NULL", Seq(nullLate)),
      ("unselected_c34_miss", "c34 = 999", Seq.empty))
    queries.foreach { case (name, predicate, expectedRows) =>
      readSpec(t, predicate = predicate, name = Some(s"read_$name")).assert { spec =>
        spec.expectation match {
          case Succeeded(result) => require(result.rowCount == expectedRows.size)
          case other => fail(s"Expected successful wide $name read, got $other")
        }
        val outputName = copiedTable.getParent.getFileName.toString
        val expectedPath = outputDir.resolve(outputName).resolve("expected")
          .resolve(s"${outputName}_read_$name").resolve("expected_data")
        RowComparison.assertRowsEqual(spark.createDataFrame(expectedRows.asJava, schema),
          spark.read.parquet(expectedPath.toString), s"independent_oracle_wide_$name")
      }
    }
    snapshotSpec(t).assert { spec =>
      spec.expectation match {
        case Succeeded(result) =>
          require(DataType.fromJson(result.metadata.schemaString) == schema)
          require(result.metadata.configuration == properties)
          require(result.metadata.partitionColumns.isEmpty)
        case other => fail(s"Expected successful wide snapshot, got $other")
      }
    }
  }

  test("alter_selected_stats_columns") {
    val schema = new StructType().add("event_ts", TimestampType)
      .add("customer", new StructType().add("country", StringType).add("segment", StringType))
      .add("amount", DecimalType(10, 2)).add("note", StringType)
    val oldDuplicate = Row(Timestamp.from(Instant.parse("2024-01-01T00:00:00Z")),
      Row("US", "retail"), BigDecimal("10.50").bigDecimal, "alpha")
    val oldOther = Row(Timestamp.from(Instant.parse("2024-01-02T00:00:00Z")),
      Row("CA", "wholesale"), BigDecimal("20.00").bigDecimal, "beta")
    val newDuplicate = Row(Timestamp.from(Instant.parse("2024-01-03T00:00:00Z")),
      Row("FR", "retail"), BigDecimal("30.25").bigDecimal, "gamma")
    val newOther = Row(Timestamp.from(Instant.parse("2024-01-04T00:00:00Z")),
      Row("US", "business"), BigDecimal("-5.00").bigDecimal, "alpha")
    val nullRow = Row(null, Row(null, null), null, null)
    val before = Seq(oldDuplicate, oldDuplicate, oldOther)
    val after = Seq(newDuplicate, newDuplicate, newOther, nullRow)
    val rows = before ++ after
    val property = "delta.dataSkippingStatsColumns"
    val properties = Map("delta.enableDeletionVectors" -> "false",
      "delta.checkpointInterval" -> "100", property -> "note,customer.segment")
    val selected = "event_ts,customer.country,amount"
    sql("""CREATE TABLE tbl (event_ts TIMESTAMP, customer STRUCT<country: STRING, segment: STRING>,
      amount DECIMAL(10, 2), note STRING) USING delta TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'false', 'delta.checkpointInterval' = '100',
        'delta.dataSkippingStatsColumns' = 'note,customer.segment')""")
    sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (TIMESTAMP '2024-01-01T00:00:00Z', named_struct('country','US','segment','retail'),
        10.50, 'alpha'),
      (TIMESTAMP '2024-01-01T00:00:00Z', named_struct('country','US','segment','retail'),
        10.50, 'alpha'),
      (TIMESTAMP '2024-01-02T00:00:00Z', named_struct('country','CA','segment','wholesale'),
        20.00, 'beta') AS v(event_ts, customer, amount, note)""")
    sql(s"ALTER TABLE tbl SET TBLPROPERTIES ('$property' = '$selected')")
    sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (TIMESTAMP '2024-01-03T00:00:00Z', named_struct('country','FR','segment','retail'),
        30.25, 'gamma'),
      (TIMESTAMP '2024-01-03T00:00:00Z', named_struct('country','FR','segment','retail'),
        30.25, 'gamma'),
      (TIMESTAMP '2024-01-04T00:00:00Z', named_struct('country','US','segment','business'),
        -5.00, 'alpha'),
      (NULL, named_struct('country',NULL,'segment',NULL), NULL, NULL)
        AS v(event_ts, customer, amount, note)""")
    val t = registerTable("tbl")
    var copiedTable: Path = null
    mutateTable(t) { dir =>
      copiedTable = dir
      val initial = CommitLog.read(dir, 0).collect { case m: Metadata => m }.head
      val changed = CommitLog.read(dir, 2)
      val metadata = changed.collect { case m: Metadata => m }.head
      require(initial.configuration == properties)
      require(metadata.configuration == properties.updated(property, selected))
      require(initial.id == metadata.id && initial.schemaString == metadata.schemaString)
      require(DataType.fromJson(metadata.schemaString) == schema)
      require(initial.partitionColumns.isEmpty && metadata.partitionColumns.isEmpty)
      require(!changed.exists(a => a.isInstanceOf[AddFile] || a.isInstanceOf[RemoveFile]),
        "ALTER must change metadata without rewriting files or backfilling statistics")
      for ((version, batch) <- Seq(1L -> before, 3L -> after)) {
        val actions = CommitLog.read(dir, version)
        require(!actions.exists(_.isInstanceOf[RemoveFile]))
        val adds = actions.collect { case a: AddFile => a }
        require(adds.size == 1 && adds.head.deletionVector.isEmpty)
        val add = adds.head
        RowComparison.assertRowsEqual(spark.createDataFrame(batch.asJava, schema),
          spark.read.parquet(dir.resolve(add.path).toString), s"selected_stats_input_v$version")
        val stats = JsonUtil.mapper.readTree(add.stats.get)
        require(stats.path("numRecords").asLong(-1) == batch.size)
        for (field <- Seq("minValues", "maxValues", "nullCount")) {
          val expectedFields = if (version == 1) Set("customer", "note")
            else Set("event_ts", "customer", "amount")
          require(stats.path(field).fieldNames().asScala.toSet == expectedFields)
          require(stats.path(field).path("customer").fieldNames().asScala.toSet ==
            (if (version == 1) Set("segment") else Set("country")))
        }
      }
    }
    val queries = Seq[(String, java.lang.Long, String, Seq[Row])](
      ("before_all", 1L, null, before),
      ("before_timestamp", 1L, "event_ts = TIMESTAMP '2024-01-01T00:00:00Z'",
        Seq(oldDuplicate, oldDuplicate)),
      ("all", null, null, rows),
      ("selected_timestamp_equal", null, "event_ts = TIMESTAMP '2024-01-01T00:00:00Z'",
        Seq(oldDuplicate, oldDuplicate)),
      ("selected_timestamp_range", null, "event_ts >= TIMESTAMP '2024-01-03T00:00:00Z'",
        Seq(newDuplicate, newDuplicate, newOther)),
      ("selected_country_equal", null, "customer.country = 'US'",
        Seq(oldDuplicate, oldDuplicate, newOther)),
      ("selected_country_null", null, "customer.country IS NULL", Seq(nullRow)),
      ("selected_amount_equal", null, "amount = 30.25", Seq(newDuplicate, newDuplicate)),
      ("selected_amount_range", null, "amount >= 10.50 AND amount <= 20.00", before),
      ("selected_amount_null", null, "amount IS NULL", Seq(nullRow)),
      ("selected_country_miss", null, "customer.country = 'ZZ'", Seq.empty),
      ("excluded_note_equal", null, "note = 'alpha'", Seq(oldDuplicate, oldDuplicate, newOther)),
      ("excluded_segment_equal", null, "customer.segment = 'retail'",
        Seq(oldDuplicate, oldDuplicate, newDuplicate, newDuplicate)),
      ("excluded_note_null", null, "note IS NULL", Seq(nullRow)),
      ("selected_combined", null,
        "event_ts >= TIMESTAMP '2024-01-03T00:00:00Z' AND " +
          "customer.country = 'FR' AND amount = 30.25", Seq(newDuplicate, newDuplicate)))
    queries.foreach { case (name, version, predicate, expectedRows) =>
      readSpec(t, version = version, predicate = predicate, name = Some(s"read_$name"))
        .assert { spec =>
          spec.expectation match {
            case Succeeded(result) => require(result.rowCount == expectedRows.size)
            case other => fail(s"Expected successful selected-stats $name read, got $other")
          }
          val outputName = copiedTable.getParent.getFileName.toString
          val expectedPath = outputDir.resolve(outputName).resolve("expected")
            .resolve(s"${outputName}_read_$name").resolve("expected_data")
          RowComparison.assertRowsEqual(spark.createDataFrame(expectedRows.asJava, schema),
            spark.read.parquet(expectedPath.toString), s"independent_oracle_selected_stats_$name")
        }
    }
    for (version <- Seq[java.lang.Long](1L, null)) {
      snapshotSpec(t, version = version).assert { spec =>
        spec.expectation match {
          case Succeeded(result) =>
            // This asserts the table schema and properties, not Kernel's internal stats schema.
            require(DataType.fromJson(result.metadata.schemaString) == schema)
            require(result.metadata.configuration ==
              (if (version == null) properties.updated(property, selected) else properties))
            require(result.metadata.partitionColumns.isEmpty)
          case other => fail(s"Expected successful selected-stats snapshot, got $other")
        }
      }
    }
  }

  // One checkpoint matrix serves both missing evolved-column stats and nullable evolution.
  // Commit/time-travel coverage stays in DataSkippingSuite and SchemaEvolutionSuite.
  for (parsed <- Seq(false, true)) {
    val variant = if (parsed) "parsed" else "json_only"
    test(s"nullable_evolution_checkpoint_$variant") {
      val schema = new StructType().add("id", IntegerType).add("part", IntegerType)
        .add("value", IntegerType)
      val oldRows = Seq(Row(1, 0, null), Row(2, 1, null), Row(2, 1, null))
      val newRows = Seq(Row(3, 0, 30), Row(3, 0, 30), Row(4, 1, null), Row(5, 1, 50))
      val rows = oldRows ++ newRows
      sql(s"""CREATE TABLE tbl (id INT, part INT) USING delta PARTITIONED BY (part)
        TBLPROPERTIES ('delta.checkpointInterval' = '100',
          'delta.enableDeletionVectors' = 'false',
          'delta.checkpoint.writeStatsAsJson' = '${!parsed}',
          'delta.checkpoint.writeStatsAsStruct' = '$parsed')""")
      sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
        (1, 0), (2, 1), (2, 1) AS v(id, part)""")
      sql("ALTER TABLE tbl ADD COLUMN value INT")
      sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
        (3, 0, 30), (3, 0, 30), (4, 1, NULL), (5, 1, 50) AS v(id, part, value)""")
      forceCheckpoint("tbl")
      val t = registerTable("tbl")
      var copiedTable: Path = null

      mutateTable(t) { dir =>
        copiedTable = dir
        val commits = (0L to 3L).map(v => v -> CommitLog.read(dir, v)).toMap
        require(!commits.values.flatten.exists(_.isInstanceOf[RemoveFile]))
        val evolved = commits(2).collect { case m: Metadata => m }.head
        require(DataType.fromJson(evolved.schemaString) == schema)
        require(evolved.partitionColumns == Seq("part"))
        val oldAdds = commits(1).collect { case a: AddFile => a }
        val newAdds = commits(3).collect { case a: AddFile => a }
        val adds = oldAdds ++ newAdds
        require(oldAdds.size == 2 && newAdds.size == 2)
        require(adds.map(_.path).distinct.size == 4, "All four files must remain active")
        for ((batch, files) <- Seq(oldRows -> oldAdds, newRows -> newAdds)) {
          require(files.map(_.partitionValues).toSet ==
            Set(Map("part" -> "0"), Map("part" -> "1")))
          files.foreach { add =>
            val part = add.partitionValues("part").toInt
            val raw = spark.read.parquet(dir.resolve(add.path).toString)
            require(raw.columns.contains("value") == (batch == newRows))
            val withValue = if (batch == oldRows) raw.withColumn("value", lit(null)
              .cast(IntegerType)) else raw
            RowComparison.assertRowsEqual(
              spark.createDataFrame(batch.filter(_.getInt(1) == part).asJava, schema),
              withValue.withColumn("part", lit(part)).select("id", "part", "value"),
              s"input_$variant/${add.path}")
            val stats = JsonUtil.mapper.readTree(add.stats.get)
            require(stats.path("numRecords").asLong(-1) == batch.count(_.getInt(1) == part))
            for (field <- Seq("minValues", "maxValues", "nullCount")) {
              require(stats.path(field).has("value") == (batch == newRows),
                s"$field must omit the evolved column only for old files")
            }
          }
        }

        val logDir = dir.resolve("_delta_log")
        val checkpoint = logDir.resolve("00000000000000000003.checkpoint.parquet")
        require(Files.isRegularFile(checkpoint), "Expected the final classic checkpoint")
        val cp = spark.read.parquet(checkpoint.toString)
        val addSchema = cp.schema("add").dataType.asInstanceOf[StructType]
        require(addSchema.fieldNames.contains("stats") == !parsed)
        require(addSchema.fieldNames.contains("stats_parsed") == parsed)
        require(addSchema.fieldNames.contains("partitionValues_parsed") == parsed)
        val cpAdds = cp.filter("add IS NOT NULL").select("add.*").collect()
        require(cpAdds.length == adds.size, "Checkpoint must contain every active AddFile")
        require(cpAdds.map(_.getAs[String]("path")).toSet == adds.map(_.path).toSet)
        require(cp.filter("remove IS NOT NULL").count() == 0)
        cpAdds.foreach { action =>
          val original = adds.find(_.path == action.getAs[String]("path")).get
          val stats = JsonUtil.mapper.readTree(original.stats.get)
          if (parsed) {
            val partition = action.getAs[Row]("partitionValues_parsed")
            require(partition.schema("part").dataType == IntegerType)
            require(partition.getAs[Int]("part") == original.partitionValues("part").toInt)
            val actual = action.getAs[Row]("stats_parsed")
            require(actual.getAs[Long]("numRecords") == stats.path("numRecords").asLong())
            for (field <- Seq("minValues", "maxValues", "nullCount")) {
              val values = actual.getAs[Row](field)
              require(values.getAs[Any]("id").toString == stats.path(field).path("id").asText())
              if (oldAdds.exists(_.path == original.path)) {
                require(values.isNullAt(values.fieldIndex("value")),
                  s"Old file $field must have no parsed evolved-column statistic")
              } else {
                require(values.getAs[Any]("value").toString ==
                  stats.path(field).path("value").asText())
              }
            }
          } else {
            require(JsonUtil.mapper.readTree(action.getAs[String]("stats")) == stats)
          }
        }
        val lastCheckpoint = JsonUtil.mapper.readTree(
          Files.readAllBytes(logDir.resolve("_last_checkpoint")))
        require(lastCheckpoint.path("version").asLong(-1) == 3)
        require(lastCheckpoint.path("size").asLong(-1) == cp.count())

        // Keep the checkpoint-version commit for timestamp resolution, but remove its entire
        // preceding history and every version CRC. Reconstruction now requires this checkpoint.
        for (v <- 0L until 3L) Files.delete(CommitLog.commitFile(dir, v))
        val stream = Files.list(logDir)
        val logFiles = try stream.iterator().asScala.toVector finally stream.close()
        logFiles.filter(_.getFileName.toString.matches("[0-9]{20}\\.crc"))
          .foreach(Files.delete)
        require((0L until 3L).forall(v => !Files.exists(CommitLog.commitFile(dir, v))))
        require(Files.exists(CommitLog.commitFile(dir, 3)))
        require(Files.exists(checkpoint) && adds.forall(a => Files.exists(dir.resolve(a.path))))
      }

      val queries = Seq[(String, String, Option[Seq[String]], Seq[Row])](
        ("all", null, None, rows),
        ("projection", null, Some(Seq("value")),
          Seq(Row(null), Row(null), Row(null), Row(30), Row(30), Row(null), Row(50))),
        ("equal", "value = 30", None, Seq(Row(3, 0, 30), Row(3, 0, 30))),
        ("range", "value >= 30 AND value < 50", None, Seq(Row(3, 0, 30), Row(3, 0, 30))),
        ("is_null", "value IS NULL", None, oldRows :+ Row(4, 1, null)),
        ("is_not_null", "value IS NOT NULL", None,
          Seq(Row(3, 0, 30), Row(3, 0, 30), Row(5, 1, 50))),
        ("partition_null", "part = 1 AND value IS NULL", None,
          Seq(Row(2, 1, null), Row(2, 1, null), Row(4, 1, null))))
      queries.foreach { case (name, predicate, columns, expectedRows) =>
        readSpec(t, predicate = predicate, columns = columns, name = Some(s"read_$name"))
          .assert { spec =>
            spec.expectation match {
              case Succeeded(result) => require(result.rowCount == expectedRows.size)
              case other => fail(s"Expected successful $variant/$name read, got $other")
            }
            val expectedSchema = if (columns.isDefined) {
              new StructType().add("value", IntegerType)
            } else schema
            val outputName = copiedTable.getParent.getFileName.toString
            val expectedPath = outputDir.resolve(outputName).resolve("expected")
              .resolve(s"${outputName}_read_$name").resolve("expected_data")
            RowComparison.assertRowsEqual(
              spark.createDataFrame(expectedRows.asJava, expectedSchema),
              spark.read.parquet(expectedPath.toString), s"independent_oracle_$variant/$name")
          }
      }
      snapshotSpec(t).assert { spec =>
        spec.expectation match {
          case Succeeded(result) =>
            require(DataType.fromJson(result.metadata.schemaString) == schema)
            require(result.metadata.partitionColumns == Seq("part"))
            require(result.metadata.configuration("delta.checkpoint.writeStatsAsStruct") ==
              parsed.toString)
            require(result.metadata.configuration("delta.checkpoint.writeStatsAsJson") ==
              (!parsed).toString)
          case other => fail(s"Expected successful $variant snapshot, got $other")
        }
      }
    }
  }
}
