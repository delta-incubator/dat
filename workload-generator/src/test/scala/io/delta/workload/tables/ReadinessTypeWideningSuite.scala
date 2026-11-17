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

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.types.{DataType, StructType}

import io.delta.workload.{TableHandle, WorkloadContext, WorkloadTestSuite}
import io.delta.workload.engine.RowComparison
import io.delta.workload.json.JsonUtil
import io.delta.workload.log.AddFile
import io.delta.workload.model._

/** Widened JSON statistics, old parsed statistics, and partition values are separate workloads. */
class ReadinessTypeWideningSuite extends WorkloadTestSuite("readiness_type_widening") {

  private case class Stage(dataType: String, values: Seq[String])
  private case class Widening(name: String, stages: Seq[Stage], jsonGap: Boolean = true)

  // The numeric chain, decimal physical encodings, and DATE values follow TypeWideningSuite.
  // One transition corpus drives all three representations. The chain's SHORT stage also covers
  // BYTE -> SHORT; existing full/time-travel JSON reads are not redeclared here.
  private val widenings = Seq(
    Widening("byte_int", Seq(
      Stage("BYTE", Seq("1", "127")), Stage("INT", Seq("128", "2147483647")))),
    Widening("byte_long", Seq(
      Stage("BYTE", Seq("1", "127")),
      Stage("BIGINT", Seq("128", "9223372036854775807")))),
    Widening("short_int", Seq(
      Stage("SHORT", Seq("10", "100")), Stage("INT", Seq("40000", "50000"))),
      jsonGap = false),
    Widening("short_long", Seq(
      Stage("SHORT", Seq("100", "32767")),
      Stage("BIGINT", Seq("40000", "9223372036854775807")))),
    Widening("int_long", Seq(
      Stage("INT", Seq("100", "200", "300", "400")),
      Stage("BIGINT", Seq("3000000000", "9000000000000"))), jsonGap = false),
    Widening("byte_double", Seq(
      Stage("BYTE", Seq("1", "127")),
      Stage("DOUBLE", Seq("127.5", "2147483648.25")))),
    Widening("short_double", Seq(
      Stage("SHORT", Seq("100", "32767")),
      Stage("DOUBLE", Seq("32767.5", "2147483648.25")))),
    Widening("int_double", Seq(
      Stage("INT", Seq("100", "2147483647")),
      Stage("DOUBLE", Seq("2147483647.5", "4294967296.25")))),
    Widening("float_double", Seq(
      Stage("FLOAT", Seq("1.5", "3.125")),
      Stage("DOUBLE", Seq("3.141592653589793", "1.7976931348623157E308"))),
      jsonGap = false),
    Widening("decimal_precision", Seq(
      Stage("DECIMAL(5,2)", Seq("123.45", "999.99")),
      Stage("DECIMAL(10,2)", Seq("12345678.90", "99999999.99"))), jsonGap = false),
    Widening("decimal_precision_scale", Seq(
      Stage("DECIMAL(9,2)", Seq("123.45", "9999999.99")),
      Stage("DECIMAL(18,2)", Seq("1234567890123.45", "9999999999999999.99")),
      Stage("DECIMAL(28,3)", Seq("1234567890123456789012345.678")))),
    Widening("date_timestamp_ntz", Seq(
      Stage("DATE", Seq("'2024-01-15'", "'2024-06-30'")),
      Stage("TIMESTAMP_NTZ", Seq("'2024-12-31 23:59:59'", "'2025-01-01 12:30:00'")))),
    Widening("multiple_steps", Seq(
      Stage("BYTE", Seq("1", "100")), Stage("SHORT", Seq("200", "30000")),
      Stage("INT", Seq("40000", "2000000000")),
      Stage("BIGINT", Seq("3000000000", "9000000000000000000")))))

  // (stable id, literal, original type). Expected bags are selected explicitly by id, never by
  // filtering the table or capturing a fresh query result as its own oracle.
  private def rows(w: Widening, stage: Int, partitioned: Boolean): Seq[(Int, String, String)] =
    w.stages.take(stage + 1).zipWithIndex.flatMap { case (s, i) =>
      s.values.zipWithIndex.map { case (v, j) => (i * 10 + j + 1, v, s.dataType) } ++
        (if (partitioned) Seq((i * 10 + 9, "NULL", s.dataType)) else Seq.empty)
    }

  private def literal(value: String, from: String, to: String): String =
    s"CAST(CAST($value AS $from) AS $to)"

  private def checkedRead(
      t: TableHandle,
      label: String,
      dataType: String,
      expected: Seq[(Int, String, String)],
      predicate: String,
      version: Long): Unit = {
    val table = WorkloadContext.current.getTableSpec(t)
    readSpec(t, version = version, predicate = predicate, name = Some(label)).assert { spec =>
      spec.expectation match {
        case Succeeded(_) =>
        case other => fail(s"${table.outputName}/$label must succeed: $other")
      }
      val selects = expected.map { case (id, value, from) =>
        s"SELECT CAST($id AS INT) AS id, ${literal(value, from, dataType)} AS value"
      }
      val oracle = if (selects.nonEmpty) selects.mkString(" UNION ALL ") else
        s"SELECT CAST(NULL AS INT) AS id, CAST(NULL AS $dataType) AS value WHERE false"
      val expectedPath = outputDir.resolve(table.outputName).resolve("expected")
        .resolve(s"${table.outputName}_$label").resolve("expected_data")
      RowComparison.assertRowsEqual(spark.sql(oracle),
        spark.read.parquet(expectedPath.toString), s"${table.outputName}/$label independent oracle")
    }
  }

  private def checkedSnapshot(t: TableHandle, w: Widening, stage: Int,
      partitioned: Boolean, version: Long): Unit = {
    snapshotSpec(t, version = version).assert { spec =>
      spec.expectation match {
        case Succeeded(result) =>
          val schema = DataType.fromJson(result.metadata.schemaString).asInstanceOf[StructType]
          val expected = StructType.fromDDL(s"id INT, value ${w.stages(stage).dataType}")
          require(schema.map(f => f.name -> f.dataType) ==
            expected.map(f => f.name -> f.dataType), s"Unexpected schema: $schema")
          require(result.metadata.partitionColumns ==
            (if (partitioned) Seq("value") else Seq.empty))
          val fieldMetadata = schema("value").metadata
          val changes = if (fieldMetadata.contains("delta.typeChanges"))
            fieldMetadata.getMetadataArray("delta.typeChanges").toSeq else Seq.empty
          val expectedChanges = w.stages.take(stage + 1).sliding(2).collect {
            case Seq(from, to) =>
              (DataType.fromDDL(from.dataType).json, DataType.fromDDL(to.dataType).json)
          }.toSeq
          require(changes.map(c => (s"\"${c.getString("fromType")}\"",
            s"\"${c.getString("toType")}\"")) == expectedChanges,
            s"Unexpected widening history at $version: $changes")
        case other => fail(s"Snapshot at $version must succeed: $other")
      }
    }
  }

  private def prepareStats(t: TableHandle, representation: String, original: Stage): Unit = {
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      val stream = Files.list(logDir)
      val paths = try stream.iterator().asScala.toVector finally stream.close()
      // CRC file-state snapshots must not substitute for the statistics under test.
      paths.filter(_.getFileName.toString.endsWith(".crc")).foreach(Files.delete)
      if (representation == "parsed") {
        val checkpoint = logDir.resolve("00000000000000000001.checkpoint.parquet")
        require(Files.exists(checkpoint), "Missing old-schema checkpoint")
        val adds = spark.read.parquet(checkpoint.toString).filter("add IS NOT NULL")
        val stats = adds.selectExpr("add.stats_parsed.minValues.value AS minValue",
          "add.stats_parsed.maxValues.value AS maxValue", "add.stats_parsed.numRecords AS records")
        require(stats.schema.head.dataType == DataType.fromDDL(original.dataType),
          s"Checkpoint must retain ${original.dataType} statistics: ${stats.schema}")
        val addSchema = adds.schema("add").dataType.asInstanceOf[StructType]
        if (addSchema.fieldNames.contains("stats")) {
          require(adds.filter("add.stats IS NOT NULL").count() == 0,
            "Parsed-stat checkpoint must not contain JSON statistics")
        }
        val oracle = s"""SELECT CAST(${original.values.head} AS ${original.dataType}) AS minValue,
          CAST(${original.values.last} AS ${original.dataType}) AS maxValue,
          CAST(${original.values.size} AS BIGINT) AS records"""
        RowComparison.assertRowsEqual(spark.sql(oracle), stats.selectExpr(
          "MIN(minValue) AS minValue", "MAX(maxValue) AS maxValue", "SUM(records) AS records"),
          "old parsed checkpoint min/max/count oracle")
        // Keep the checkpoint's boundary commit for version discovery; its adds are removed below.
        Files.delete(logDir.resolve("00000000000000000000.json"))
      } else {
        require(!paths.exists(_.getFileName.toString.contains("checkpoint")),
          "JSON/partition workloads must not have checkpoints")
        val actions = paths.filter(_.getFileName.toString.endsWith(".json"))
          .flatMap(p => Files.readAllLines(p).asScala)
          .map(JsonUtil.mapper.readTree(_))
        val adds = actions.filter(_.has("add")).map(_.get("add"))
        require(adds.nonEmpty)
        if (representation == "json") {
          require(adds.forall { a =>
            a.hasNonNull("stats") && Seq("minValues", "maxValues").forall { bound =>
              JsonUtil.mapper.readTree(a.get("stats").asText()).path(bound).has("value")
            }
          })
        } else {
          require(adds.forall(_.get("partitionValues").has("value")))
          require(adds.exists(_.get("partitionValues").get("value").isNull),
            "Retain a null partition written before widening")
        }
      }
    }
    if (representation == "parsed") {
      modifyCommitActions(t, version = 1)(_.filterNot(_.isInstanceOf[AddFile]))
    }
  }

  for {
    w <- widenings
    representation <- Seq("json", "parsed", "partition")
    if representation != "json" || w.jsonGap
  } {
    test(s"${representation}_${w.name}") {
      val partitioned = representation == "partition"
      val partition = if (partitioned) "PARTITIONED BY (value)" else ""
      val timestampFeature = if (w.name == "date_timestamp_ntz")
        ", 'delta.feature.timestampNtz' = 'supported'" else ""
      val statsProperties = if (representation == "parsed")
        ", 'delta.checkpoint.writeStatsAsStruct' = 'true', " +
          "'delta.checkpoint.writeStatsAsJson' = 'false'" else ""
      sql(s"""CREATE TABLE tbl (id INT, value ${w.stages.head.dataType}) USING delta
        $partition TBLPROPERTIES ('delta.enableTypeWidening' = 'true',
          'delta.enableDeletionVectors' = 'true', 'delta.checkpointInterval' = '100',
          'delta.checkpointPolicy' = 'classic' $timestampFeature $statsProperties)""")

      for ((s, i) <- w.stages.zipWithIndex) {
        if (i > 0) sql(s"ALTER TABLE tbl ALTER COLUMN value TYPE ${s.dataType}")
        val inserted = rows(w, i, partitioned).filter(_._1 / 10 == i)
        val values = inserted.map { case (id, value, from) =>
          s"($id, CAST($value AS $from))"
        }.mkString(", ")
        sql(s"INSERT INTO tbl VALUES $values")
        if (i == 0 && representation == "parsed") forceCheckpoint("tbl")
      }

      val t = registerTable("tbl")
      prepareStats(t, representation, w.stages.head)

      // Capture the requested latest-schema assertions before historical-stage controls.
      for ((s, i) <- w.stages.zipWithIndex.reverse) {
        val version = i * 2L + 1
        checkedSnapshot(t, w, i, partitioned, version)
        val precisionOnlyStats = representation != "partition" &&
          w.name == "decimal_precision_scale" && i == 1
        if (i > 0 && !precisionOnlyStats) {
          val bag = rows(w, i, partitioned)
          val nonNull = bag.filter(_._2 != "NULL")
          val original = nonNull.filter(_._1 < 10)
          val previous = w.stages(i - 1)
          val firstNew = literal(s.values.head, s.dataType, s.dataType)
          val lastOld = literal(previous.values.last, previous.dataType, s.dataType)
          val lastOriginal = literal(w.stages.head.values.last,
            w.stages.head.dataType, s.dataType)
          val firstOriginal = literal(w.stages.head.values.head,
            w.stages.head.dataType, s.dataType)
          val prefix = if (w.name == "multiple_steps" && i == 1) "byte_short" else w.name
          val label = s"${prefix}_v$version"
          val read = (suffix: String, predicate: String, selected: Seq[(Int, String, String)]) =>
            checkedRead(t, s"${label}_$suffix", s.dataType, selected, predicate, version)

          // JSON partial cases contribute filters only; TypeWideningSuite owns their full reads.
          val partialJson = representation == "json" &&
            Set("multiple_steps", "decimal_precision_scale", "date_timestamp_ntz").contains(w.name)
          if (!partialJson) read("all", null, bag)
          read("old_range", s"value <= $lastOriginal", original)
          read("new_range", s"value > $lastOld", nonNull.filter(_._1 / 10 == i))
          read("cross_range", s"value >= $lastOld AND value <= $firstNew",
            nonNull.filter(r => r._1 == (i - 1) * 10 + previous.values.size ||
              r._1 == i * 10 + 1))
          read("miss", s"value < $firstOriginal", Seq.empty)

          if (partitioned) {
            read("old_equality", s"value = $firstOriginal", original.take(1))
            read("new_equality", s"value = $firstNew", nonNull.filter(_._1 == i * 10 + 1))
            read("null_partitions", "value IS NULL", bag.filter(_._2 == "NULL"))
          }
          if (w.name == "date_timestamp_ntz") {
            read("old_midnight", s"value = $lastOriginal", original.takeRight(1))
            read("new_midnight_miss", "value = TIMESTAMP_NTZ'2024-12-31 00:00:00'", Seq.empty)
            read("exclusive_boundary", s"value >= $lastOriginal AND value < $firstNew",
              original.takeRight(1))
          }
        }
      }
    }
  }
}
