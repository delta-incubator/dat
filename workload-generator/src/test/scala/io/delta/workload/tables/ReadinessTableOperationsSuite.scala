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

import java.io.DataInputStream
import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}

import io.delta.workload.{WorkloadContext, WorkloadTestSuite}
import io.delta.workload.deltaharness.{DeltaHarness, Format => HarnessFormat}
import io.delta.workload.engine.{RowComparison, SnapshotResolver}
import io.delta.workload.json.JsonUtil
import io.delta.workload.log.{Action, AddFile, CommitLog, DeletionVector, DomainMetadata, Metadata,
  Protocol, RemoveFile, Txn}
import io.delta.workload.model._

class ReadinessTableOperationsSuite extends WorkloadTestSuite("readiness_table_operations") {

  private def descriptor(dv: DeletionVector): DeletionVectorDescriptor = {
    // At this DAT pin USE_LONG_FOR_INTS boxes Option[Int] offsets as Long. Normalize
    // only this suite's descriptor input, without changing the shared mapper or DV bytes.
    val offset = dv.offset.asInstanceOf[Option[Number]].map(n => Math.toIntExact(n.longValue()))
    DeletionVectorDescriptor(dv.storageType, dv.pathOrInlineDv, offset,
      dv.sizeInBytes, dv.cardinality)
  }

  private def sidecar(table: Path, dv: DeletionVector): Path =
    Paths.get(descriptor(dv).absolutePath(new HadoopPath(table.toUri)).toUri.getPath)

  private def payload(table: Path, dv: DeletionVector): Array[Byte] = {
    if (dv.storageType == "i") descriptor(dv).inlineData
    else {
      assert(dv.storageType == "u")
      val file = sidecar(table, dv)
      assert(file.normalize().startsWith(table.normalize()))
      val offset = descriptor(dv).offset.get
      assert(offset > 0)
      assert(Files.size(file) >= offset.toLong + 4L + dv.sizeInBytes + 4L)
      val input = new DataInputStream(Files.newInputStream(file))
      try {
        assert(input.readByte() == DeletionVectorStore.DV_FILE_FORMAT_VERSION_ID_V1)
        input.skipNBytes(offset.toLong - 1L)
        // This OSS reader checks both the payload length and checksum.
        DeletionVectorStore.readRangeFromStream(input, dv.sizeInBytes)
      } finally input.close()
    }
  }

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

  test("e2e_51_stats_refresh") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta TBLPROPERTIES (
      'delta.enableDeletionVectors' = 'true', 'delta.checkpointInterval' = '1000',
      'delta.checkpointPolicy' = 'classic')""")
    sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (-7, 'drop'), (0, 'low'), (4, CAST(NULL AS STRING)), (4, CAST(NULL AS STRING)),
      (12, 'middle'), (18, CAST(NULL AS STRING)), (31, 'last'),
      (CAST(NULL AS INT), 'missing') AS v(id, value)""")
    sql("DELETE FROM tbl WHERE id = -7")
    val table = registerTable("tbl")
    val schema = new StructType().add("id", IntegerType).add("value", StringType)
    val physicalRows = Seq(Row(-7, "drop"), Row(0, "low"), Row(4, null), Row(4, null),
      Row(12, "middle"), Row(18, null), Row(31, "last"), Row(null, "missing"))
    val rows = Seq(Row(0, "low"), Row(4, null), Row(4, null), Row(12, "middle"),
      Row(18, null), Row(31, "last"), Row(null, "missing"))
    val original = (0L to 2L).map(v => v -> CommitLog.read(table.sourcePath, v)).toMap
    val metadata = original(0L).collect { case m: Metadata => m }.ensuring(_.size == 1).head
    val protocol = Protocol(3, 7, Some(Set("deletionVectors")),
      Some(Set("appendOnly", "deletionVectors", "invariants")))
    assert(original(0L).collect { case p: Protocol => p } == Seq(protocol))
    assert(DataType.fromJson(metadata.schemaString) == schema)
    assert(metadata.partitionColumns.isEmpty)
    assert(metadata.configuration == Map("delta.enableDeletionVectors" -> "true",
      "delta.checkpointInterval" -> "1000", "delta.checkpointPolicy" -> "classic"))
    val inserted = original(1L).collect { case a: AddFile => a }.ensuring(_.size == 1).head
    val deleted = original(2L).collect { case a: AddFile => a }.ensuring(_.size == 1).head
    assert(inserted.path == deleted.path && inserted.deletionVector.isEmpty)
    val dv = deleted.deletionVector.get
    assert(dv.storageType == "u" && dv.cardinality == 1L)
    val physical = spark.read.parquet(table.sourcePath.resolve(deleted.path).toString)
    assert(physical.schema == schema)
    RowComparison.assertRowsEqual(spark.createDataFrame(physicalRows.asJava, schema), physical,
      "replay literal physical rows")
    val positions = physical.collect().toSeq.zipWithIndex.collect {
      case (row, index) if !row.isNullAt(0) && row.getInt(0) == -7 => index.toLong
    }
    val dvBytes = payload(table.sourcePath, dv)
    assert(RoaringBitmapArray.readFrom(dvBytes).toArray.toSeq == positions)
    val fileBytes = Files.readAllBytes(table.sourcePath.resolve(deleted.path))
    // Physical stats include the deleted row. These bounds remain conservative for the DV.
    // Model a partial-stats writer followed by a stats-only update, not an autostats service.
    val partialStats = """{"numRecords":8,"tightBounds":false}"""
    val refreshedStats = """{"numRecords":8,"minValues":{"id":-7,"value":"drop"},
      "maxValues":{"id":31,"value":"missing"},"nullCount":{"id":1,"value":3},
      "tightBounds":false}"""
    val partial = deleted.copy(stats = Some(partialStats))
    val refreshed = partial.copy(stats = Some(refreshedStats), dataChange = false)
    assert(refreshed.copy(stats = deleted.stats, dataChange = deleted.dataChange) == deleted)
    assert(refreshed.path == deleted.path && refreshed.deletionVector == Some(dv))
    assert(refreshed.baseRowId == deleted.baseRowId)
    assert(refreshed.defaultRowCommitVersion == deleted.defaultRowCommitVersion)
    // Row tracking is not enabled: preserve absent row-ID fields as well as the real DV.
    assert(deleted.baseRowId.isEmpty && deleted.defaultRowCommitVersion.isEmpty)
    modifyCommitActions(table, 2L) { actions =>
      actions.map { case _: AddFile => partial; case other => other }
    }
    var copiedTable: Path = null
    mutateTable(table) { dir =>
      copiedTable = dir
      assert(dir.getParent.getParent == outputDir)
      assert(CommitLog.read(dir, 0L) == original(0L))
      assert(CommitLog.read(dir, 1L) == original(1L))
      assert(CommitLog.read(dir, 2L) == original(2L).map {
        case _: AddFile => partial; case other => other
      })
      assert(!Files.exists(CommitLog.commitFile(dir, 3L)))
      // One AddFile per identity per version; no conflicting same-version remove/add pair.
      CommitLog.write(dir, 3L, Seq(refreshed))
      val logs = Files.list(dir.resolve("_delta_log"))
      try logs.iterator().asScala.filter(_.getFileName.toString.endsWith(".crc"))
        .foreach(Files.delete)
      finally logs.close()
      DeltaHarness.get.openLog(spark, dir.toString).checkpoint(3L)
      val checkpoint = dir.resolve("_delta_log/00000000000000000003.checkpoint.parquet")
      assert(Files.isRegularFile(checkpoint))
      val checkpointAdds = spark.read.parquet(checkpoint.toString)
        .filter("add IS NOT NULL").select("add.*").toJSON.collect().toSeq
        .map(json => Action.parse(s"""{"add":$json}"""))
      assert(checkpointAdds == Seq(refreshed))
      val lastCheckpoint = JsonUtil.mapper.readTree(
        Files.readAllBytes(dir.resolve("_delta_log/_last_checkpoint")))
      assert(lastCheckpoint.get("version").asLong() == 3L)
      // Checkpoint creation can write fresh Hadoop sidecars; retain only protocol artifacts.
      val checkpointLogs = Files.list(dir.resolve("_delta_log"))
      try checkpointLogs.iterator().asScala.filter(_.getFileName.toString.endsWith(".crc"))
        .foreach { file =>
          assert(file.getFileName.toString.startsWith("."))
          Files.delete(file)
        }
      finally checkpointLogs.close()
      // A transaction marker provides a genuine JSON tail without changing the row bag.
      CommitLog.write(dir, 4L, Seq(Txn("readiness_e2e_51_tail", 1L)))
      assert(CommitLog.read(dir, 3L) == Seq(refreshed))
      assert(CommitLog.read(dir, 4L) == Seq(Txn("readiness_e2e_51_tail", 1L)))
      assert(Files.readAllBytes(dir.resolve(deleted.path)).sameElements(fileBytes))
      assert(payload(dir, dv).sameElements(dvBytes))
      DeltaLog.clearCache()
      val replay = DeltaLog.forTable(spark, dir.toString).update()
      assert(replay.version == 4L)
      assert(replay.logSegment.checkpointProvider.version == 3L)
      assert(replay.logSegment.deltas.map(_.getPath.getName) == Seq(f"${4L}%020d.json"))
      (2L to 4L).foreach { version =>
        val snapshot = DeltaHarness.get.openLog(spark, dir.toString).getSnapshotAt(version)
        val active = snapshot.allFiles.select("json").collect().toSeq
          .map(r => Action.parse(r.getString(0)))
        // Delta's active-file replay normalizes dataChange; commit flags were checked above.
        val expectedAdd = (if (version == 2L) partial else refreshed).copy(dataChange = false)
        assert(active == Seq(expectedAdd))
      }
      val remaining = Files.list(dir.resolve("_delta_log"))
      try assert(!remaining.iterator().asScala.exists(_.getFileName.toString.endsWith(".crc")))
      finally remaining.close()
      info("replay checkpoint v3 + JSON tail v4 selected; physical file and DV unchanged")
    }
    val expectedSnapshot = SnapshotResult(
      ProtocolInfo(3, 7, Some(Seq("deletionVectors")),
        Some(Seq("appendOnly", "deletionVectors", "invariants"))),
      MetadataInfo(metadata.id, metadata.name, metadata.description,
        HarnessFormat("parquet", Map.empty), schema.json, Seq.empty,
        metadata.configuration, metadata.createdTime))
    Seq[java.lang.Long](2L, 3L, 4L, null).foreach { version =>
      snapshotSpec(table, version = version).assert { spec =>
        val resolved = Option(version).fold(4L)(_.toLong)
        assert(spec.query == SnapshotQuery(version = Some(resolved)))
        assert(spec.expectation == Succeeded(if (resolved == 4L) {
          expectedSnapshot.copy(setTransactions = Some(Seq(AppTxn("readiness_e2e_51_tail", 1L))))
        } else expectedSnapshot))
      }
    }
    val projected = new StructType().add("value", StringType).add("id", IntegerType)
    val selective = "id IN (4, 12) OR id IS NULL"
    val selected = Seq(Row(null, 4), Row(null, 4), Row("middle", 12), Row("missing", null))
    val versions = Seq("before" -> Some(2L), "refreshed" -> Some(3L),
      "tail" -> Some(4L), "latest" -> None)
    versions.foreach { case (label, version) =>
      val cases = Seq(
        (s"read_$label", ReadQuery(version = version), schema, rows),
        (s"read_${label}_selective", ReadQuery(version = version,
          predicate = Some(selective), columns = Some(Seq("value", "id"))), projected, selected),
        (s"read_${label}_miss", ReadQuery(version = version, predicate = Some("id > 100")),
          schema, Seq.empty[Row]))
      cases.foreach { case (name, query, expectedSchema, expectedRows) =>
        readSpec(table, name = Some(name), version = query.version.map(Long.box).orNull,
          predicate = query.predicate.orNull, columns = query.columns).assert { spec =>
          assert(spec.query == query)
          spec.expectation match {
            case Succeeded(result) => assert(result.rowCount == expectedRows.size.toLong)
            case other => fail(s"replay $name expected Succeeded, got $other")
          }
          val expected = spark.createDataFrame(expectedRows.asJava, expectedSchema)
          val actual = SnapshotResolver.applyFilters(
            SnapshotResolver.buildDeltaReader(spark, copiedTable, query.version, query.timestamp),
            query.predicate, query.columns)
          assert(actual.schema == expectedSchema)
          RowComparison.assertRowsEqual(expected, actual, s"replay $name copied table")
          val fixture = copiedTable.getParent
          val specName = s"${fixture.getFileName}_$name"
          val captured = spark.read.parquet(
            fixture.resolve(s"expected/$specName/expected_data").toString)
          assert(captured.schema == expectedSchema)
          RowComparison.assertRowsEqual(expected, captured, s"replay $name expected parquet")
        }
      }
    }
  }

  Seq("zorder", "liquid").foreach { mode =>
    test(s"e2e_49_$mode") {
      val liquid = mode == "liquid"
      val clusterBy = if (liquid) "CLUSTER BY (id, value)" else ""
      sql(s"""CREATE TABLE tbl (id INT, value STRING) USING delta $clusterBy TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'false', 'delta.checkpointInterval' = '1000',
        'delta.checkpointPolicy' = 'classic')""")
      sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
        (8, 'late'), (3, CAST(NULL AS STRING)), (3, CAST(NULL AS STRING)),
        (CAST(NULL AS INT), 'missing') AS v(id, value)""")
      sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
        (-2, 'early'), (5, 'middle'), (8, 'late'), (12, CAST(NULL AS STRING)) AS v(id, value)""")
      val table = registerTable("tbl")
      val schema = new StructType().add("id", IntegerType).add("value", StringType)
      // Independent literal bag: includes duplicate whole rows and nulls in both columns.
      val firstRows = Seq(Row(8, "late"), Row(3, null), Row(3, null), Row(null, "missing"))
      val allRows = firstRows ++ Seq(Row(-2, "early"), Row(5, "middle"), Row(8, "late"),
        Row(12, null))
      val before = (0L to 2L).map(v => v -> CommitLog.read(table.sourcePath, v)).toMap
      val metadata = before(0L).collect { case m: Metadata => m }.ensuring(_.size == 1).head
      val protocol = if (liquid) {
        Protocol(1, 7, None, Some(Set("appendOnly", "invariants", "clustering", "domainMetadata")))
      } else Protocol(1, 2)
      assert(before(0L).collect { case p: Protocol => p } == Seq(protocol))
      assert(DataType.fromJson(metadata.schemaString) == schema)
      assert(metadata.format == io.delta.workload.log.Format())
      assert(metadata.partitionColumns.isEmpty)
      assert(metadata.configuration == Map("delta.enableDeletionVectors" -> "false",
        "delta.checkpointInterval" -> "1000", "delta.checkpointPolicy" -> "classic"))
      val domains = before(0L).collect { case d: DomainMetadata => d }
      if (liquid) {
        assert(domains.size == 1 && domains.head.domain == "delta.clustering")
        assert(!domains.head.removed)
        val configuration = JsonUtil.mapper.readTree(domains.head.configuration)
        assert(configuration.get("clusteringColumns") ==
          JsonUtil.mapper.readTree("""[["id"],["value"]]"""))
      } else assert(domains.isEmpty)
      val literal = spark.createDataFrame(allRows.asJava, schema)
      val unoptimized = SnapshotResolver.buildDeltaReader(spark, table.sourcePath,
        Some(2L), None)
      assert(unoptimized.schema == schema)
      RowComparison.assertRowsEqual(literal, unoptimized, s"$mode before OPTIMIZE")
      sql(if (liquid) "OPTIMIZE tbl" else "OPTIMIZE tbl ZORDER BY (id, value)")
      val optimized = CommitLog.read(table.sourcePath, 3L)
      val adds = optimized.collect { case a: AddFile => a }
      val removes = optimized.collect { case r: RemoveFile => r }
      // Check actual rewrite actions, without promising a file count or layout effectiveness.
      assert(adds.nonEmpty && removes.nonEmpty)
      assert(adds.forall(!_.dataChange) && removes.forall(!_.dataChange))
      val inputAdds = before.values.flatten.collect { case a: AddFile => a }.toSeq
      assert(removes.map(_.path).toSet == inputAdds.map(_.path).toSet)
      assert(adds.forall(_.clusteringProvider == (if (liquid) Some("liquid") else None)))
      assert(optimized.collect { case m: Metadata => m }.isEmpty)
      assert(optimized.collect { case p: Protocol => p }.isEmpty)
      assert(optimized.collect { case d: DomainMetadata => d }.isEmpty)
      val commitInfo = optimized.map(a => JsonUtil.mapper.readTree(a.toJson))
        .filter(_.has("commitInfo")).ensuring(_.size == 1).head.get("commitInfo")
      assert(commitInfo.get("operation").asText() == "OPTIMIZE")
      val parameters = commitInfo.get("operationParameters")
      val columns = JsonUtil.mapper.readTree("""["id","value"]""")
      val empty = JsonUtil.mapper.readTree("[]")
      assert(JsonUtil.mapper.readTree(parameters.get("zOrderBy").asText()) ==
        (if (liquid) empty else columns))
      assert(JsonUtil.mapper.readTree(parameters.get("clusterBy").asText()) ==
        (if (liquid) columns else empty))
      info(s"cluster $mode protocol: " + protocol.toJson)
      info(s"cluster $mode metadata: " + metadata.toJson)
      info(s"cluster $mode optimize: " + commitInfo.toString)
      domains.foreach(d => info(s"cluster $mode domain: " + d.toJson))
      adds.foreach(a => info(s"cluster $mode optimized add: " + a.toJson))
      var copiedTable: Path = null
      mutateTable(table) { dir =>
        copiedTable = dir
        (0L to 2L).foreach(v => assert(CommitLog.read(dir, v) == before(v)))
        assert(CommitLog.read(dir, 3L) == optimized)
      }
      val expectedSnapshot = SnapshotResult(
        ProtocolInfo(protocol.minReaderVersion, protocol.minWriterVersion,
          writerFeatures = protocol.writerFeatures.map(_.toSeq.sorted)),
        MetadataInfo(metadata.id, metadata.name, metadata.description,
          HarnessFormat("parquet", Map.empty), schema.json, Seq.empty,
          metadata.configuration, metadata.createdTime),
        domainMetadata = if (liquid) Some(domains.map(d =>
          AddDomainMetadata(d.domain, d.configuration))) else None)
      Seq[java.lang.Long](0L, 2L, 3L, null).foreach { version =>
        snapshotSpec(table, version = version).assert { spec =>
          assert(spec.query == SnapshotQuery(version = Some(Option(version).fold(3L)(_.toLong))))
          assert(spec.expectation == Succeeded(expectedSnapshot))
        }
      }
      val projected = new StructType().add("value", StringType).add("id", IntegerType)
      val predicate = "(id >= 3 AND id <= 8) OR id IS NULL"
      val firstFiltered = Seq(Row("late", 8), Row(null, 3), Row(null, 3), Row("missing", null))
      val allFiltered = firstFiltered ++ Seq(Row("middle", 5), Row("late", 8))
      val cases = Seq(
        ("read_latest", ReadQuery(), schema, allRows),
        ("read_first_insert", ReadQuery(version = Some(1L)), schema, firstRows),
        ("read_before_optimize", ReadQuery(version = Some(2L)), schema, allRows),
        ("read_optimized_v3", ReadQuery(version = Some(3L)), schema, allRows),
        ("read_first_filtered", ReadQuery(version = Some(1L), predicate = Some(predicate),
          columns = Some(Seq("value", "id"))), projected, firstFiltered),
        ("read_latest_filtered", ReadQuery(predicate = Some(predicate),
          columns = Some(Seq("value", "id"))), projected, allFiltered),
        ("read_optimized_filtered", ReadQuery(version = Some(3L), predicate = Some(predicate),
          columns = Some(Seq("value", "id"))), projected, allFiltered))
      cases.foreach { case (name, query, expectedSchema, rows) =>
        readSpec(table, name = Some(name), version = query.version.map(Long.box).orNull,
          predicate = query.predicate.orNull, columns = query.columns).assert { spec =>
          assert(spec.query == query)
          spec.expectation match {
            case Succeeded(result) => assert(result.rowCount == rows.size.toLong)
            case other => fail(s"$mode $name expected Succeeded, got $other")
          }
          val expected = spark.createDataFrame(rows.asJava, expectedSchema)
          val actual = SnapshotResolver.applyFilters(
            SnapshotResolver.buildDeltaReader(spark, copiedTable, query.version, query.timestamp),
            query.predicate, query.columns)
          assert(actual.schema == expectedSchema)
          RowComparison.assertRowsEqual(expected, actual, s"$mode $name copied table")
          val fixture = outputDir.resolve(WorkloadContext.current.workloadName)
          val specName = s"${fixture.getFileName}_$name"
          val captured = spark.read.parquet(
            fixture.resolve(s"expected/$specName/expected_data").toString)
          assert(captured.schema == expectedSchema)
          RowComparison.assertRowsEqual(expected, captured, s"$mode $name expected parquet")
        }
      }
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

  test("e2e_58_unicode_路径") {
    // The generator copies contents into <fixture>/delta. Relative log paths must
    // survive both the Unicode source root and that different destination root.
    val parent = Files.createTempDirectory("paths-source-")
    val source = parent.resolve("源_café_表_😀")
    sql(s"""CREATE TABLE tbl (id INT, value STRING) USING delta LOCATION '$source'
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'false',
      'delta.checkpointInterval' = '1000', 'delta.checkpointPolicy' = 'classic')""")
    sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (-7, 'base'), (4, CAST(NULL AS STRING)), (4, CAST(NULL AS STRING)),
      (CAST(NULL AS INT), 'missing') AS v(id, value)""")
    sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (4, CAST(NULL AS STRING)), (23, 'end'),
      (CAST(NULL AS INT), CAST(NULL AS STRING)) AS v(id, value)""")
    // Canonically equivalent NFC/NFD basenames must remain distinct byte names.
    val basenames = Seq("données_café_表_😀.parquet", "données_cafe\u0301_表_😀.parquet")
    assert(basenames.distinct.size == 2)
    val schema = new StructType().add("id", IntegerType).add("value", StringType)
    val first = Seq(Row(-7, "base"), Row(4, null), Row(4, null), Row(null, "missing"))
    val second = Seq(Row(4, null), Row(23, "end"), Row(null, null))
    val inputRows = Seq(first, second)
    val renamedAdds = (1L to 2L).map { v =>
      val add = CommitLog.read(source, v).collect { case a: AddFile => a }
        .ensuring(_.size == 1).head
      assert(!add.path.contains("/") && add.deletionVector.isEmpty)
      val basename = basenames((v - 1).toInt)
      val file = source.resolve(basename)
      Files.move(source.resolve(add.path), file)
      // Change only the file reference in raw JSON; preserve every other action field.
      val commit = CommitLog.commitFile(source, v)
      val lines = Files.readAllLines(commit).asScala.map { line =>
        val node = JsonUtil.mapper.readTree(line)
        if (node.has("add")) {
          node.get("add").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
            .put("path", new HadoopPath(basename).toUri.toString)
        }
        node.toString
      }
      Files.write(commit, lines.asJava)
      val physical = spark.read.parquet(file.toString)
      assert(physical.schema == schema)
      RowComparison.assertRowsEqual(spark.createDataFrame(inputRows((v - 1).toInt).asJava,
        schema), physical, s"paths source physical v$v")
      val renamed = CommitLog.read(source, v).collect { case a: AddFile => a }.head
      assert(new HadoopPath(renamed.path).toUri.getPath == basename)
      assert(Files.size(file) == renamed.size)
      renamed
    }
    // Version checksums can embed the old paths; do not transport stale checksums.
    val logs = Files.list(source.resolve("_delta_log"))
    try logs.iterator().asScala.filter(_.getFileName.toString.endsWith(".crc"))
      .foreach(Files.delete)
    finally logs.close()
    val commits = (0L to 2L).map(v => v -> Files.readAllBytes(
      CommitLog.commitFile(source, v))).toMap
    val metadata = CommitLog.read(source, 0L).collect { case m: Metadata => m }
      .ensuring(_.size == 1).head
    val configuration = Map("delta.enableDeletionVectors" -> "false",
      "delta.checkpointInterval" -> "1000", "delta.checkpointPolicy" -> "classic")
    assert(CommitLog.read(source, 0L).collect { case p: Protocol => p } == Seq(Protocol(1, 2)))
    assert(DataType.fromJson(metadata.schemaString) == schema)
    assert(metadata.partitionColumns.isEmpty && metadata.configuration == configuration)
    val expectedSnapshot = SnapshotResult(ProtocolInfo(1, 2),
      MetadataInfo(metadata.id, metadata.name, metadata.description,
        HarnessFormat("parquet", Map.empty), schema.json, Seq.empty,
        configuration, metadata.createdTime))
    val table = registerTableFromPath(source.toString)
    var copiedTable: Path = null
    mutateTable(table) { dir =>
      copiedTable = dir
      assert(dir != source && dir.getParent.getFileName.toString.endsWith("路径"))
      commits.foreach { case (v, bytes) =>
        assert(Files.readAllBytes(CommitLog.commitFile(dir, v)).sameElements(bytes))
        assert(!new String(bytes, java.nio.charset.StandardCharsets.UTF_8)
          .contains(source.toString))
      }
      basenames.foreach { basename =>
        val file = dir.resolve(basename)
        assert(file.getFileName.toString.getBytes(java.nio.charset.StandardCharsets.UTF_8)
          .sameElements(basename.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
        assert(Files.readAllBytes(file).sameElements(Files.readAllBytes(source.resolve(basename))))
      }
      // Remove the entire source before any snapshot/read capture. An absolute or
      // stale reference to it cannot make the relocated fixture pass by accident.
      org.apache.commons.io.FileUtils.deleteDirectory(parent.toFile)
      assert(!Files.exists(parent) && !Files.exists(source))
      DeltaLog.clearCache()
      info(s"paths source removed: $source; relocated root: $dir")
    }
    Seq[java.lang.Long](0L, 1L, 2L, null).foreach { version =>
      val v = Option(version).fold(2L)(_.toLong)
      snapshotSpec(table, version = version).assert { spec =>
        assert(!Files.exists(source))
        assert(spec.query == SnapshotQuery(version = Some(v)))
        assert(spec.expectation == Succeeded(expectedSnapshot))
        val snapshot = DeltaLog.forTable(spark, copiedTable.toString).getSnapshotAt(v)
        assert(snapshot.allFiles.collect().map(_.path).toSet ==
          renamedAdds.take(v.toInt).map(_.path).toSet)
        assert(snapshot.logSegment.checkpointProvider.version == -1L)
      }
    }
    val projected = new StructType().add("value", StringType).add("id", IntegerType)
    val cases = Seq(
      ("read_v1", ReadQuery(version = Some(1L)), schema, first, 1),
      ("read_v2", ReadQuery(version = Some(2L)), schema, first ++ second, 2),
      ("read_latest", ReadQuery(), schema, first ++ second, 2),
      ("read_selective", ReadQuery(predicate = Some("id = 4 OR id IS NULL"),
        columns = Some(Seq("value", "id"))), projected,
        Seq(Row(null, 4), Row(null, 4), Row(null, 4), Row("missing", null), Row(null, null)), 2))
    cases.foreach { case (name, query, expectedSchema, rows, files) =>
      readSpec(table, name = Some(name), version = query.version.map(Long.box).orNull,
        predicate = query.predicate.orNull, columns = query.columns).assert { spec =>
        assert(!Files.exists(source))
        assert(spec.query == query)
        assert(spec.expectation == Succeeded(ReadResult(rows.size.toLong, files, 0L)))
        val actual = SnapshotResolver.applyFilters(
          SnapshotResolver.buildDeltaReader(spark, copiedTable, query.version, query.timestamp),
          query.predicate, query.columns)
        val paths = actual.inputFiles.map(p => Paths.get(java.net.URI.create(p))).toSet
        val expectedFiles = basenames.take(files).map(copiedTable.resolve).toSet
        assert(paths == expectedFiles && paths.forall(_.startsWith(copiedTable)))
        assert(actual.schema == expectedSchema)
        val expected = spark.createDataFrame(rows.asJava, expectedSchema)
        RowComparison.assertRowsEqual(expected, actual, s"paths $name relocated table")
        val fixture = copiedTable.getParent
        val captured = spark.read.parquet(fixture.resolve(
          s"expected/${fixture.getFileName}_$name/expected_data").toString)
        assert(captured.schema == expectedSchema)
        RowComparison.assertRowsEqual(expected, captured, s"paths $name expected parquet")
      }
    }
  }
}
