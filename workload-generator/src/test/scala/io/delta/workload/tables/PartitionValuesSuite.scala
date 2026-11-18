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

import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetReader}
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType, TimestampType}

import io.delta.workload.{WorkloadContext, WorkloadTestSuite}
import io.delta.workload.engine.RowComparison
import io.delta.workload.json.JsonUtil
import io.delta.workload.log.{AddFile, CommitLog}
import io.delta.workload.model.Succeeded

class PartitionValuesSuite extends WorkloadTestSuite("partition_values")
    with ScopedSessionConf with LiteralOracleChecks {

  test("boolean") {
    sql("""CREATE TABLE tbl (id INT, flag BOOLEAN) USING delta
      PARTITIONED BY (flag) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, true), (2, false), (3, true), (4, false)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "flag = true", name = Some("filter_true"))
    readSpec(t, predicate = "flag = false", name = Some("filter_false"))
    snapshotSpec(t)
  }

  test("byte") {
    sql("""CREATE TABLE tbl (id INT, b BYTE) USING delta
      PARTITIONED BY (b) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(-128 AS BYTE)), (2, CAST(0 AS BYTE)), (3, CAST(127 AS BYTE)), (4, CAST(1 AS BYTE))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "b = CAST(-128 AS BYTE)", name = Some("filter_min"))
    readSpec(t, predicate = "b = CAST(127 AS BYTE)", name = Some("filter_max"))
    readSpec(t, predicate = "b > CAST(0 AS BYTE)", name = Some("filter_positive"))
    snapshotSpec(t)
  }

  test("decimal") {
    sql("""CREATE TABLE tbl (id INT, amount DECIMAL(10,2)) USING delta
      PARTITIONED BY (amount) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 99.99), (2, -100.50), (3, 0.01), (4, 12345.67)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "amount = 99.99", name = Some("filter_eq"))
    readSpec(t, predicate = "amount > 0", name = Some("filter_positive"))
    readSpec(t, predicate = "amount = -100.50", name = Some("filter_boundary"))
    snapshotSpec(t)
  }

  test("double") {
    sql("""CREATE TABLE tbl (id INT, d DOUBLE) USING delta
      PARTITIONED BY (d) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 3.14159), (2, -2.71828), (3, 1000000.001), (4, 0.0)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "d > 0", name = Some("filter_positive"))
    readSpec(t, predicate = "d > 100", name = Some("filter_large"))
    snapshotSpec(t)
  }

  test("empty_string") {
    sql("""CREATE TABLE tbl (id INT, tag STRING) USING delta
      PARTITIONED BY (tag) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, ''), (2, 'hello'), (3, CAST(NULL AS STRING)), (4, 'world')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "tag = ''", name = Some("filter_empty_string"))
    readSpec(t, predicate = "tag IS NULL", name = Some("filter_null"))
    readSpec(t, predicate = "tag IS NOT NULL AND tag != ''", name = Some("filter_nonempty"))
    snapshotSpec(t)
  }

  test("float") {
    sql("""CREATE TABLE tbl (id INT, f FLOAT) USING delta
      PARTITIONED BY (f) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(1.5 AS FLOAT)), (2, CAST(-3.14 AS FLOAT)), (3, CAST(0.0 AS FLOAT)), (4, CAST(99.9 AS FLOAT))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "f = CAST(1.5 AS FLOAT)", name = Some("filter_eq"))
    readSpec(t, predicate = "f > CAST(0.0 AS FLOAT)", name = Some("filter_positive"))
    snapshotSpec(t)
  }

  test("multi_cols") {
    sql("""CREATE TABLE tbl (id INT, value STRING, p_str STRING, p_int INT, p_bool BOOLEAN) USING delta
      PARTITIONED BY (p_str, p_int, p_bool) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, 'a', 'cat_a', 10, true), (2, 'b', 'cat_a', 20, false),
      (3, 'c', 'cat_b', 10, true), (4, 'd', 'cat_b', 30, false),
      (5, 'e', 'cat_a', 10, false), (6, 'f', 'cat_c', 40, true)""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "p_str = 'cat_a'", name = Some("filter_str"))
    readSpec(t, predicate = "p_str = 'cat_a' AND p_int = 10", name = Some("filter_str_and_int"))
    readSpec(t, predicate = "p_int >= 20 AND p_int <= 30", name = Some("filter_int_range"))
    readSpec(t, predicate = "p_bool = true", name = Some("filter_bool_only"))
    readSpec(t, predicate = "p_str = 'cat_b' AND p_int = 10 AND p_bool = true", name = Some("filter_all_three"))
    snapshotSpec(t)
  }

  test("null_value") {
    sql("""CREATE TABLE tbl (id INT, category STRING) USING delta
      PARTITIONED BY (category) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'A'), (2, CAST(NULL AS STRING)), (3, 'B'), (4, CAST(NULL AS STRING))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "category = 'A'", name = Some("filter_eq"))
    readSpec(t, predicate = "category IS NULL", name = Some("filter_null"))
    readSpec(t, predicate = "category IS NOT NULL", name = Some("filter_not_null"))
    snapshotSpec(t)
  }

  test("short") {
    sql("""CREATE TABLE tbl (id INT, s SHORT) USING delta
      PARTITIONED BY (s) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(-32768 AS SHORT)), (2, CAST(0 AS SHORT)), (3, CAST(32767 AS SHORT)), (4, CAST(100 AS SHORT))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "s = CAST(-32768 AS SHORT)", name = Some("filter_min"))
    readSpec(t, predicate = "s = CAST(32767 AS SHORT)", name = Some("filter_max"))
    readSpec(t, predicate = "s >= CAST(0 AS SHORT) AND s <= CAST(100 AS SHORT)", name = Some("filter_range"))
    snapshotSpec(t)
  }

  test("special_chars") {
    sql("""CREATE TABLE tbl (id INT, label STRING) USING delta
      PARTITIONED BY (label) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, 'hello world'), (2, 'caf\u00e9'), (3, 'a/b=c&d'), (4, 'normal')""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "label = 'hello world'", name = Some("filter_space"))
    readSpec(t, predicate = "label = 'caf\u00e9'", name = Some("filter_unicode"))
    readSpec(t, predicate = "label = 'a/b=c&d'", name = Some("filter_special"))
    snapshotSpec(t)
  }

  test("timestamp_ntz") {
    sql("""CREATE TABLE tbl (id INT, ts_ntz TIMESTAMP_NTZ) USING delta
      PARTITIONED BY (ts_ntz) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'2024-01-01 00:00:00'), (2, TIMESTAMP_NTZ'2024-06-15 12:30:00'),
      (3, TIMESTAMP_NTZ'2024-12-31 23:59:59')""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "ts_ntz = TIMESTAMP_NTZ'2024-06-15 12:30:00'", name = Some("filter_eq"))
    readSpec(t, predicate = "ts_ntz >= TIMESTAMP_NTZ'2024-06-01 00:00:00'", name = Some("filter_range"))
    snapshotSpec(t)
  }

  test("timestamp") {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP) USING delta
      PARTITIONED BY (ts) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP'2024-01-01 00:00:00'), (2, TIMESTAMP'2024-06-15 12:30:00.123456'),
      (3, TIMESTAMP'2024-12-31 23:59:59')""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "ts = TIMESTAMP'2024-06-15 12:30:00.123456'", name = Some("filter_eq"))
    readSpec(t, predicate = "ts >= TIMESTAMP'2024-06-01 00:00:00'", name = Some("filter_range"))
    snapshotSpec(t)
  }

  // Partition values come from AddFile metadata, not from data file directories.
  test("metadata_partition_values_ignore_directories") {
    sql("""CREATE TABLE tbl (id INT, payload STRING, part STRING) USING delta
      PARTITIONED BY (part) TBLPROPERTIES ('delta.checkpointInterval' = '1000')""")
    sql("""INSERT INTO tbl VALUES
      (1, 'duplicate', 'east'), (1, 'duplicate', 'east'),
      (2, 'conflicting', 'west'), (3, 'null_partition', NULL),
      (4, 'no_hive_directory', 'plain')""")
    val t = registerTable("tbl")
    val table = WorkloadContext.current.getTableSpec(t)
    val schema = new StructType().add("id", IntegerType).add("payload", StringType)
      .add("part", StringType)

    mutateTable(t) { tableDir =>
      val originals = CommitLog.read(tableDir, 1L).collect { case add: AddFile => add }
      assert(originals.nonEmpty)
      assert(originals.map(_.partitionValues("part")).toSet == Set("east", "west", null, "plain"))
      CommitLog.mutate(tableDir, 1L) { actions =>
        actions.map {
          case add: AddFile =>
            val directory = if (add.partitionValues("part") == "plain") "opaque/nested"
              else "part=directory_lie"
            val original = tableDir.resolve(add.path)
            val relative = s"$directory/${original.getFileName}"
            val target = tableDir.resolve(relative)
            Files.createDirectories(target.getParent)
            Files.move(original, target)
            assert(!Files.exists(original) && Files.isRegularFile(target))
            assert(Files.size(target) == add.size)
            add.copy(path = relative)
          case other => other
        }
      }
      val moved = CommitLog.read(tableDir, 1L).collect { case add: AddFile => add }
      assert(moved.map(_.partitionValues) == originals.map(_.partitionValues))
      assert(moved.zip(originals).forall { case (after, before) =>
        after.copy(path = before.path) == before
      })
      assert(moved.forall(add => Files.isRegularFile(tableDir.resolve(add.path))))
      assert(moved.forall(add => !originals.exists(_.path == add.path)))
      // Require JSON replay: no checkpoint or Delta CRC may retain the original file paths.
      val stream = Files.list(tableDir.resolve("_delta_log"))
      val files = try stream.iterator().asScala.toSeq finally stream.close()
      assert(!files.exists(_.getFileName.toString.contains("checkpoint")))
      files.filter(_.getFileName.toString.endsWith(".crc")).foreach(Files.delete)
      println("Metadata-partition layout oracle passed: conflicting and non-Hive directories")
    }

    val rows = Seq(Row(1, "duplicate", "east"), Row(1, "duplicate", "east"),
      Row(2, "conflicting", "west"), Row(3, "null_partition", null),
      Row(4, "no_hive_directory", "plain"))
    val reads = Seq(
      ("read_all", null, rows.indices, None),
      ("read_metadata_east", "part = 'east'", Seq(0, 1), None),
      ("read_metadata_west", "part = 'west'", Seq(2), None),
      ("read_directory_lie", "part = 'directory_lie'", Seq.empty[Int], None),
      ("read_null", "part IS NULL", Seq(3), None),
      ("read_no_hive_directory", "part = 'plain'", Seq(4), None),
      ("read_projection", null, rows.indices, Some(Seq("part", "id"))),
      ("read_null_projection", "part IS NULL", Seq(3), Some(Seq("part", "payload"))))
    for ((name, predicate, indices, columns) <- reads) {
      readSpec(t, name = Some(name), predicate = predicate, columns = columns).assert { spec =>
        spec.expectation match {
          case Succeeded(result) => assert(result.rowCount == indices.size.toLong)
          case other => fail(s"metadata partition/$name must succeed: $other")
        }
        val literal = spark.createDataFrame(indices.map(rows).asJava, schema)
        val oracle = columns.map(cs => literal.selectExpr(cs: _*)).getOrElse(literal)
        val expected = outputDir.resolve(table.outputName).resolve("expected")
          .resolve(s"${table.outputName}_$name").resolve("expected_data")
        val captured = spark.read.parquet(expected.toString)
        RowComparison.assertRowsEqual(oracle, captured, s"metadata partition/$name literal bag")
        assert(captured.schema == oracle.schema)
        println(s"Metadata-partition literal row/schema oracle passed: $name")
      }
    }
    snapshotSpec(t).assert { spec =>
      spec.expectation match {
        case Succeeded(result) =>
          assert(DataType.fromJson(result.metadata.schemaString) == schema)
          assert(result.metadata.partitionColumns == Seq("part"))
          assert(result.metadata.configuration == Map("delta.checkpointInterval" -> "1000"))
        case other => fail(s"metadata partition snapshot must succeed: $other")
      }
      println("Metadata-partition snapshot schema/partition oracle passed")
    }
  }

  // Literal AddFile partition strings: empty values, timestamp serializations, and
  // canonical DATE calendar bounds.
  // Literal empty AddFile partition strings are independent of writer normalization.
  // BYTE is numeric TINYINT. Its empty input has no numeric value and is expected to be null.
  // STRING and BINARY retain a distinct empty value; writer support was probed separately.
  for ((kind, sqlType, valueType, firstValue, secondValue, control, emptyValue) <- Seq(
      ("byte", "TINYINT", org.apache.spark.sql.types.ByteType,
        "CAST(1 AS TINYINT)", "CAST(2 AS TINYINT)", 2.toByte, null),
      ("short", "SMALLINT", org.apache.spark.sql.types.ShortType,
        "CAST(1 AS SMALLINT)", "CAST(2 AS SMALLINT)", 2.toShort, null),
      ("int", "INT", IntegerType, "1", "2", 2, null),
      ("long", "BIGINT", org.apache.spark.sql.types.LongType, "1L", "2L", 2L, null),
      ("float", "FLOAT", org.apache.spark.sql.types.FloatType,
        "CAST(1.25 AS FLOAT)", "CAST(2.5 AS FLOAT)", 2.5f, null),
      ("double", "DOUBLE", org.apache.spark.sql.types.DoubleType,
        "CAST(1.25 AS DOUBLE)", "CAST(2.5 AS DOUBLE)", 2.5d, null),
      ("decimal", "DECIMAL(10,2)", org.apache.spark.sql.types.DecimalType(10, 2),
        "CAST(1.25 AS DECIMAL(10,2))", "CAST(2.50 AS DECIMAL(10,2))",
        new java.math.BigDecimal("2.50"), null),
      ("boolean", "BOOLEAN", org.apache.spark.sql.types.BooleanType,
        "false", "true", true, null),
      ("date", "DATE", org.apache.spark.sql.types.DateType,
        "DATE '1969-12-31'", "DATE '2000-02-29'", java.sql.Date.valueOf("2000-02-29"), null),
      ("timestamp", "TIMESTAMP", TimestampType,
        "TIMESTAMP '1969-12-31T23:59:59Z'", "TIMESTAMP '2024-03-10T10:00:00.123456Z'",
        Timestamp.from(Instant.parse("2024-03-10T10:00:00.123456Z")), null),
      ("timestamp_ntz", "TIMESTAMP_NTZ", org.apache.spark.sql.types.TimestampNTZType,
        "TIMESTAMP_NTZ '1969-12-31 23:59:59'", "TIMESTAMP_NTZ '2024-03-10 02:30:00.123456'",
        java.time.LocalDateTime.parse("2024-03-10T02:30:00.123456"), null),
      ("string", "STRING", org.apache.spark.sql.types.StringType,
        "'first'", "'second'", "second", ""),
      ("binary", "BINARY", org.apache.spark.sql.types.BinaryType,
        "unhex('61')", "unhex('62')", Array[Byte](98), Array.emptyByteArray))) {
    test(s"empty_partition_$kind") {
      setForTest("spark.sql.session.timeZone", "UTC")
      // ANSI predicate casts reject empty numeric input. Leave legacy casts active for capture.
      // The existing spec format cannot transport this session setting to an external consumer.
      setForTest("spark.sql.ansi.enabled", "false")
      val expectedSchema = new StructType().add("id", IntegerType).add("p", valueType)
      val rows = Seq(Row(1, emptyValue), Row(2, control), Row(2, control), Row(3, null))
      sql(s"CREATE TABLE tbl (id INT, p $sqlType) USING delta PARTITIONED BY (p)")
      sql(s"INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES " +
        s"(1, $firstValue), (2, $secondValue), (2, $secondValue), " +
        s"(3, CAST(NULL AS $sqlType)) AS rows(id, p)")
      val table = registerTable("tbl")
      modifyCommitActions(table, 1L) { actions =>
        actions.map {
          case add: AddFile if JsonUtil.mapper.readTree(add.stats.get)
              .at("/minValues/id").intValue() == 1 =>
            add.copy(partitionValues = Map("p" -> ""))
          case other => other
        }
      }
      mutateTable(table) { dir =>
        removeVersionChecksums(dir)
        val adds = CommitLog.read(dir, 1L).collect { case add: AddFile => add }
        require(adds.size == 3)
        require(adds.count(_.partitionValues("p") == "") == 1)
        require(adds.count(_.partitionValues("p") == null) == 1)
        println(s"Verified serialized empty partition: $kind empty=1 null=1 control=1")
      }
      checkLiteralRead(table, "all", expectedSchema, rows)
      checkLiteralRead(table, "all_v1", expectedSchema, rows, version = 1L)
      checkLiteralRead(table, "is_null", expectedSchema, rows.filter(_.isNullAt(1)),
        predicate = "p IS NULL")
      checkLiteralRead(table, "is_not_null", expectedSchema, rows.filterNot(_.isNullAt(1)),
        predicate = "p IS NOT NULL")
      checkLiteralRead(table, "eq_control", expectedSchema, rows.filter(_.getInt(0) == 2),
        predicate = s"p = $secondValue")
      if (kind == "string" || kind == "binary") {
        val emptyLiteral = if (kind == "string") "''" else "unhex('')"
        checkLiteralRead(table, "eq_empty", expectedSchema, rows.filter(_.getInt(0) == 1),
          predicate = s"p = $emptyLiteral")
      }
      snapshotSpec(table).assert { spec =>
        require(spark.conf.get("spark.sql.ansi.enabled") == "false")
        spec.expectation match {
          case Succeeded(result) =>
            require(DataType.fromJson(result.metadata.schemaString) == expectedSchema)
            require(result.metadata.partitionColumns == Seq("p"))
            require(result.metadata.format.provider == "parquet")
            require(result.metadata.format.options.isEmpty)
            require(result.protocol.minReaderVersion == (if (kind == "timestamp_ntz") 3 else 1))
            require(result.protocol.minWriterVersion == (if (kind == "timestamp_ntz") 7 else 2))
          case other => fail(s"Empty partition snapshot must succeed: $other")
        }
        println(s"Verified capture mode: ${WorkloadContext.current.workloadName} ansi=false")
      }
      checkLiteralSnapshot(table, expectedSchema, Seq("p"), version = 1L)
    }
  }

  // Inputs are literal AddFile strings, not SQL timestamp literal spellings.
  // The pinned Spark parser permits zone suffixes for NTZ casts and ignores their offsets;
  // expected NTZ wall clocks below are separate from the corresponding absolute instants.
  private def timestampPartitionFormats: Vector[(String, String, String, String)] = {
    val fractions = Seq(
      ("0", "", "000000"), ("1", ".1", "100000"), ("2", ".12", "120000"),
      ("3", ".123", "123000"), ("4", ".1234", "123400"),
      ("5", ".12345", "123450"), ("6", ".123456", "123456"),
      ("7", ".1234567", "123456"), ("8", ".12345678", "123456"),
      ("9", ".123456789", "123456"), ("dot", ".", "000000"),
      ("10", ".1234567891", "123456"), ("18", ".123456789123456789", "123456"))
    val zones = Seq(
      ("z", "12:34:56.123456Z", "12:34:56.123456"),
      ("zero_colon", "12:34:56.123456+00:00", "12:34:56.123456"),
      ("zero_basic", "12:34:56.123456+0000", "12:34:56.123456"),
      ("zero_hour", "12:34:56.123456+00", "12:34:56.123456"),
      ("minus_zero", "12:34:56.123456-00:00", "12:34:56.123456"),
      ("utc_name", "12:34:56.123456 UTC", "12:34:56.123456"),
      ("gmt_name", "12:34:56.123456 GMT", "12:34:56.123456"),
      ("east", "18:04:56.123456+05:30", "18:04:56.123456"),
      ("west", "04:34:56.123456-08:00", "04:34:56.123456"),
      ("region", "04:34:56.123456 America/Los_Angeles", "04:34:56.123456"),
      ("east_basic", "18:04:56.123456+0530", "18:04:56.123456"),
      ("east_hour", "17:34:56.123456+05", "17:34:56.123456"),
      ("west_basic", "04:34:56.123456-0800", "04:34:56.123456"),
      ("west_hour", "04:34:56.123456-08", "04:34:56.123456"),
      ("west_half", "09:04:56.123456-03:30", "09:04:56.123456"),
      ("single_hour", "18:04:56.123456+5:30", "18:04:56.123456"),
      ("single_minute", "17:37:56.123456+05:3", "17:37:56.123456"),
      ("offset_seconds", "18:05:26.123456+05:30:30", "18:05:26.123456"))
    (for ((separatorName, separator) <- Seq("space" -> " ", "T" -> "T");
        entry <- fractions.map { case (label, fraction, micros) =>
          (s"${separatorName}_fraction_$label", s"2024-02-29${separator}12:34:56$fraction",
            s"2024-02-29T12:34:56.$micros" + "Z", s"2024-02-29T12:34:56.$micros")
        } ++ zones.map { case (label, raw, wall) =>
          (s"${separatorName}_$label", s"2024-02-29$separator$raw",
            "2024-02-29T12:34:56.123456Z", s"2024-02-29T$wall")
        }) yield entry).toVector
  }

  for ((kind, sqlType, valueType) <- Seq(
      ("timestamp", "TIMESTAMP", TimestampType),
      ("timestamp_ntz", "TIMESTAMP_NTZ", org.apache.spark.sql.types.TimestampNTZType))) {
    test(s"partition_serializations_$kind") {
      setForTest("spark.sql.session.timeZone", "UTC")
      setForTest("spark.sql.ansi.enabled", "false")
      val formats = timestampPartitionFormats
      require(formats.size == 62)
      val expectedSchema = new StructType().add("id", IntegerType).add("p", valueType)
      val keys = formats.zipWithIndex.map { case ((_, _, instant, wall), index) =>
        (index + 1) -> (if (kind == "timestamp") instant else wall)
      }.toMap
      val rows0 = keys.toSeq.sortBy(_._1).map { case (id, key) =>
        val value = if (kind == "timestamp") Timestamp.from(Instant.parse(key))
          else java.time.LocalDateTime.parse(key)
        Row(id, value)
      }
      val rows = rows0 :+ rows0.head
      sql(s"CREATE TABLE tbl (id INT, p $sqlType) USING delta PARTITIONED BY (p)")
      // Distinct seed partitions create one data file per raw representation. Only id is
      // physically stored; the typed AddFile mutation supplies the independently checked p.
      val seeds = formats.indices.map { index =>
        val seed = Instant.parse("2030-01-01T00:00:00Z").plusSeconds(index.toLong).toString
        val literal = if (kind == "timestamp") seed else seed.stripSuffix("Z")
        s"(${index + 1}, $sqlType '$literal')"
      }
      sql("INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES " +
        (seeds :+ seeds.head).mkString(", ") + " AS rows(id, p)")
      val table = registerTable("tbl")
      modifyCommitActions(table, 1L) { actions =>
        actions.map {
          case add: AddFile =>
            val id = JsonUtil.mapper.readTree(add.stats.get).at("/minValues/id").intValue()
            add.copy(partitionValues = Map("p" -> formats(id - 1)._2))
          case other => other
        }
      }
      mutateTable(table) { dir =>
        removeVersionChecksums(dir)
        val adds = CommitLog.read(dir, 1L).collect { case add: AddFile => add }
        require(adds.size == formats.size)
        adds.foreach { add =>
          val stats = JsonUtil.mapper.readTree(add.stats.get)
          val id = stats.at("/minValues/id").intValue()
          require(stats.at("/maxValues/id").intValue() == id)
          require(stats.get("numRecords").asLong() == (if (id == 1) 2L else 1L))
          require(add.partitionValues == Map("p" -> formats(id - 1)._2))
          println(s"Verified raw serialization: $kind ${formats(id - 1)._1} " +
            s"value=${formats(id - 1)._2}")
        }
      }
      checkLiteralRead(table, "all", expectedSchema, rows)
      checkLiteralRead(table, "all_v1", expectedSchema, rows, version = 1L)
      keys.values.toSet.toSeq.sorted.zipWithIndex.foreach { case (key, index) =>
        val ids = keys.collect { case (id, value) if value == key => id }.toSet
        checkLiteralRead(table, s"eq_$index", expectedSchema,
          rows.filter(r => ids.contains(r.getInt(0))), predicate = s"p = $sqlType '$key'")
      }
      def selected(lower: String, upper: String): Seq[Row] = rows.filter { row =>
        val key = keys(row.getInt(0))
        key >= lower && key < upper
      }
      checkLiteralRead(table, "fraction_range", expectedSchema,
        selected("2024-02-29T12:34:56.100000", "2024-02-29T12:34:56.123456"),
        predicate = s"p >= $sqlType '2024-02-29 12:34:56.100000' AND " +
          s"p < $sqlType '2024-02-29 12:34:56.123456'")
      checkLiteralRead(table, "before_noon", expectedSchema,
        selected("2024-02-29T00:00:00", "2024-02-29T12:00:00"),
        predicate = s"p < $sqlType '2024-02-29 12:00:00'")
      checkLiteralRead(table, "miss", expectedSchema, Seq.empty,
        predicate = s"p < $sqlType '2024-02-29 00:00:00'")
      checkLiteralSnapshot(table, expectedSchema, Seq("p"))
      checkLiteralSnapshot(table, expectedSchema, Seq("p"), version = 1L)
    }
  }

  // Canonical zero-padded DATE strings use proleptic Gregorian epoch-day oracles.
  test("date_partition_canonical_calendar_bounds") {
    setForTest("spark.sql.session.timeZone", "UTC")
    setForTest("spark.sql.ansi.enabled", "false")
    // Expected Parquet must retain proleptic Gregorian days even before the 1582 cutover.
    setForTest("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    setForTest("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    val dates = Seq(
      (1, "0001-01-01", -719162), (2, "1582-10-04", -141438),
      (3, "1582-10-15", -141427), (4, "1969-12-31", -1),
      (5, "1970-01-01", 0), (6, "2000-02-29", 11016), (7, "9999-12-31", 2932896))
    val expectedSchema = new StructType().add("id", IntegerType)
      .add("p", org.apache.spark.sql.types.DateType)
    val rows = dates.map { case (id, raw, _) => Row(id, java.sql.Date.valueOf(raw)) } ++
      Seq(Row(6, java.sql.Date.valueOf("2000-02-29")), Row(8, null))
    val days: Map[Int, Option[Int]] = dates.map { case (id, _, day) =>
      id -> Some(day)
    }.toMap + (8 -> None)
    sql("CREATE TABLE tbl (id INT, p DATE) USING delta PARTITIONED BY (p)")
    val inputs = dates.map { case (id, raw, _) => s"($id, DATE '$raw')" } ++
      Seq("(6, DATE '2000-02-29')", "(8, CAST(NULL AS DATE))")
    sql("INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES " +
      inputs.mkString(", ") + " AS rows(id, p)")
    val table = registerTable("tbl")
    modifyCommitActions(table, 1L) { actions =>
      actions.map {
        case add: AddFile =>
          val id = JsonUtil.mapper.readTree(add.stats.get).at("/minValues/id").intValue()
          val raw = if (id == 8) null else dates.find(_._1 == id).get._2
          add.copy(partitionValues = Map("p" -> raw))
        case other => other
      }
    }
    mutateTable(table) { dir =>
      removeVersionChecksums(dir)
      val adds = CommitLog.read(dir, 1L).collect { case add: AddFile => add }
      require(adds.size == 8)
      adds.foreach { add =>
        val stats = JsonUtil.mapper.readTree(add.stats.get)
        val id = stats.at("/minValues/id").intValue()
        val raw = if (id == 8) null else dates.find(_._1 == id).get._2
        require(add.partitionValues == Map("p" -> raw))
        require(stats.at("/maxValues/id").intValue() == id)
        require(stats.get("numRecords").asLong() == (if (id == 6) 2L else 1L))
        if (raw != null) require(raw.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}"))
        println(s"Verified canonical DATE partition: id=$id raw=$raw epochDay=${days(id)}")
      }
    }
    val allName = "all"
    checkLiteralRead(table, allName, expectedSchema, rows)
    checkLiteralRead(table, "all_v1", expectedSchema, rows, version = 1L)
    dates.foreach { case (id, raw, _) =>
      checkLiteralRead(table, s"eq_$id", expectedSchema, rows.filter(_.getInt(0) == id),
        predicate = s"p = DATE '$raw'")
    }
    checkLiteralRead(table, "is_null", expectedSchema, rows.filter(_.isNullAt(1)),
      predicate = "p IS NULL")
    checkLiteralRead(table, "pre_epoch", expectedSchema, rows.filter(r => r.getInt(0) <= 4),
      predicate = "p < DATE '1970-01-01'")
    checkLiteralRead(table, "calendar_transition", expectedSchema,
      rows.filter(r => Set(2, 3).contains(r.getInt(0))),
      predicate = "p >= DATE '1582-10-04' AND p <= DATE '1582-10-15'")
    checkLiteralRead(table, "modern_range", expectedSchema,
      rows.filter(r => Set(5, 6).contains(r.getInt(0))),
      predicate = "p >= DATE '1970-01-01' AND p <= DATE '2000-02-29'")
    val workload = WorkloadContext.current.workloadName
    snapshotSpec(table).assert { spec =>
      require(spark.conf.get("spark.sql.parquet.datetimeRebaseModeInWrite") == "CORRECTED")
      require(spark.conf.get("spark.sql.parquet.datetimeRebaseModeInRead") == "CORRECTED")
      spec.expectation match {
        case Succeeded(result) =>
          require(DataType.fromJson(result.metadata.schemaString) == expectedSchema)
          require(result.metadata.partitionColumns == Seq("p"))
          require(result.metadata.format.provider == "parquet")
          require(result.metadata.format.options.isEmpty)
          require(result.protocol.minReaderVersion == 1 && result.protocol.minWriterVersion == 2)
        case other => fail(s"Canonical DATE snapshot must succeed: $other")
      }

    }
    checkLiteralSnapshot(table, expectedSchema, Seq("p"), version = 1L)
    // Snapshots capture before reads at this pin. Inspect Parquet in the last read callback.
    val outsideName = "outside_bounds"
    readSpec(table, predicate = "p < DATE '0001-01-01' OR p > DATE '9999-12-31'",
      name = Some(outsideName)).assert { spec =>
      spec.expectation match {
        case Succeeded(result) => require(result.rowCount == 0L)
        case other => fail(s"Outside DATE bounds must succeed with no rows: $other")
      }
      val expected = spark.createDataFrame(Seq.empty[Row].asJava, expectedSchema)
      val outside = outputDir.resolve(workload).resolve("expected")
        .resolve(s"${workload}_$outsideName").resolve("expected_data")
      RowComparison.assertRowsEqual(expected, spark.read.parquet(outside.toString), outsideName)
      println(s"Verified independent literal rows/schema: $workload/$outsideName rows=0")
      val captured = outputDir.resolve(workload).resolve("expected")
        .resolve(s"${workload}_$allName").resolve("expected_data")
      val literalOracle = outputDir.resolveSibling("oracle-parquet").resolve(workload)
      val expectedUnits = days.toVector :+ (6 -> Some(11016))
      for ((label, directory) <- Seq("captured" -> captured, "literal" -> literalOracle)) {
        val files = Files.list(directory)
        val parquet = try files.iterator().asScala.filter(_.toString.endsWith(".parquet"))
          .toVector finally files.close()
        require(parquet.nonEmpty)
        var units = Vector.empty[(Int, Option[Int])]
        parquet.foreach { file =>
          val path = new HadoopPath(file.toUri)
          val conf = spark.sparkContext.hadoopConfiguration
          val footer = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))
          try {
            val physical = footer.getFooter.getFileMetaData.getSchema.getFields.asScala
              .find(_.getName == "p").get.asPrimitiveType()
            require(physical.getPrimitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32)
            require(physical.getLogicalTypeAnnotation == LogicalTypeAnnotation.dateType())
          } finally footer.close()
          val reader = ParquetReader.builder[Group](new GroupReadSupport(), path)
            .withConf(conf).build()
          try {
            var record = reader.read()
            while (record != null) {
              val day = if (record.getFieldRepetitionCount("p") == 0) None
                else Some(record.getInteger("p", 0))
              units :+= record.getInteger("id", 0) -> day
              record = reader.read()
            }
          } finally reader.close()
        }
        require(units.sorted == expectedUnits.sorted)
        println(s"Verified physical DATE oracle: $workload $label units=${units.sorted}")
      }
    }
  }

}
