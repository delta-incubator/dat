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

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{Files, Path, StandardCopyOption}
import java.sql.Timestamp
import java.time.{Instant, LocalDate}

import scala.jdk.CollectionConverters._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.{NanoTime, SimpleGroupFactory}
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetReader}
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupReadSupport}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{DataType, IntegerType, StructType, TimestampNTZType,
  TimestampType}

import io.delta.workload.{TableHandle, WorkloadContext, WorkloadTestSuite}
import io.delta.workload.engine.RowComparison
import io.delta.workload.json.JsonUtil
import io.delta.workload.log.{AddFile, CommitLog}
import io.delta.workload.model.Succeeded

/** Timestamp stats encodings are checked on disk, independently of the table reader. */
class ReadinessTimestampSuite extends WorkloadTestSuite("readiness_timestamp") {
  private val first = "2024-01-01T00:00:00.123456Z"
  private val last = "2024-01-01T00:00:00.123789Z"
  private val nanoLower = "2024-01-01T00:00:00.123455111Z"
  private val nanoUpper = "2024-01-01T00:00:00.123789999Z"
  private val schema = new StructType().add("id", IntegerType).add("ts", TimestampType)

  // Literal instants, not values read back from the table, are the row oracle.
  private def expectedRows: Seq[Row] = Seq(
    Row(1, Timestamp.from(Instant.parse(first))),
    Row(2, Timestamp.from(Instant.parse(last))),
    Row(2, Timestamp.from(Instant.parse(last))))

  private def createTable(): TableHandle = {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP) USING delta TBLPROPERTIES (
      'delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '2',
      'delta.checkpointPolicy' = 'classic',
      'delta.checkpoint.writeStatsAsStruct' = 'true',
      'delta.checkpoint.writeStatsAsJson' = 'false')""")
    sql("""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES
      (1, TIMESTAMP '2024-01-01 00:00:00.123456'),
      (2, TIMESTAMP '2024-01-01 00:00:00.123789'),
      (2, TIMESTAMP '2024-01-01 00:00:00.123789') AS rows(id, ts)""")
    val t = registerTable("tbl")
    val adds = CommitLog.read(t.sourcePath, 1).collect { case a: AddFile => a }
    require(adds.size == 1, "The timestamp bounds must describe one file with all three rows")
    t
  }

  private def checkSpecs(t: TableHandle, checkJsonStats: Boolean = false): Unit = {
    val workloadName = WorkloadContext.current.workloadName
    val tableOutputDir = outputDir.resolve(workloadName)
    val queries = Seq(
      ("all", null, expectedRows),
      ("lower_edge", "ts <= TIMESTAMP '2024-01-01 00:00:00.123456'", expectedRows.take(1)),
      ("upper_edge", "ts >= TIMESTAMP '2024-01-01 00:00:00.123789'", expectedRows.drop(1)),
      ("outside", "ts < TIMESTAMP '2024-01-01 00:00:00.123456' OR " +
        "ts > TIMESTAMP '2024-01-01 00:00:00.123789'", Seq.empty[Row]))

    for (version <- Seq[java.lang.Long](null, 1L)) {
      val suffix = if (version == null) "latest" else "v1"
      for ((label, predicate, rows) <- queries) {
        val name = s"${label}_$suffix"
        readSpec(t, predicate = predicate, version = version, name = Some(name)).assert { spec =>
          spec.expectation match {
            case Succeeded(result) => require(result.rowCount == rows.size)
            case other => fail(s"$name must succeed: $other")
          }
          val expected = spark.createDataFrame(rows.asJava, schema)
          val expectedDir = tableOutputDir.resolve("expected")
            .resolve(s"${workloadName}_$name")
          val captured = expectedDir.resolve("expected_data")
          RowComparison.assertRowsEqual(expected, spark.read.parquet(captured.toString), name)
          if (checkJsonStats && label == "all") {
            val scanned = spark.read.parquet(
              expectedDir.resolve("expected_metadata").toString).collect()
            require(scanned.length == 1)
            val scanStats = JsonUtil.mapper.readTree(scanned.head.getString(0))
              .at("/add/stats").asText()
            val tableDir = tableOutputDir.resolve("delta")
            val rawStats = CommitLog.read(tableDir, 1)
              .collectFirst { case a: AddFile => a.stats.get }.get
            require(scanStats == rawStats, "Scanned stats must retain the injected precision")
            println(s"Verified $name scanned AddFile stats match nanosecond JSON")
          }
        }
      }
      snapshotSpec(t, version = version).assert { spec =>
        spec.expectation match {
          case Succeeded(result) =>
            require(DataType.fromJson(result.metadata.schemaString) == schema)
          case other => fail(s"$suffix snapshot must succeed: $other")
        }
      }
    }
  }

  test("nanosecond_stats_json") {
    val t = createTable()
    // The bounds enclose the microsecond rows. All of digits 7-9 are nonzero.
    val lower = nanoLower
    val upper = nanoUpper
    require(Instant.parse(lower).isBefore(Instant.parse(first)))
    require(Instant.parse(upper).isAfter(Instant.parse(last)))
    modifyCommitActions(t, version = 1) { actions =>
      actions.map {
        case a: AddFile =>
          val stats = JsonUtil.mapper.readTree(a.stats.get)
          stats.get("minValues").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
            .put("ts", lower)
          stats.get("maxValues").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
            .put("ts", upper)
          a.copy(stats = Some(JsonUtil.mapper.writeValueAsString(stats)))
        case other => other
      }
    }
    mutateTable(t) { tableDir =>
      val adds = CommitLog.read(tableDir, 1).collect { case a: AddFile => a }
      require(adds.size == 1)
      val raw = JsonUtil.mapper.readTree(adds.head.stats.get)
      require(raw.at("/minValues/ts").asText() == lower)
      require(raw.at("/maxValues/ts").asText() == upper)
      require(raw.get("numRecords").asLong() == 3)
      // A copied checksum can contain the old AddFile stats and bypass JSON parsing.
      val files = Files.list(tableDir.resolve("_delta_log"))
      try files.iterator().asScala.filter(_.toString.endsWith(".crc"))
        .foreach(Files.delete(_)) finally files.close()
      println(s"Verified JSON stats: min=$lower max=$upper records=3")
    }
    checkSpecs(t, checkJsonStats = true)
  }

  for ((caseName, encoding, lower, upper) <- Seq(
      ("millisecond_stats_checkpoint", "TIMESTAMP_MILLIS",
        "2024-01-01T00:00:00.123Z", "2024-01-01T00:00:00.124Z"),
      ("microsecond_stats_checkpoint", "TIMESTAMP_MICROS", first, last),
      ("int96_stats_checkpoint", "INT96", first, last))) {
    test(caseName) {
      val t = createTable()
      require(!Instant.parse(lower).isAfter(Instant.parse(first)))
      require(!Instant.parse(upper).isBefore(Instant.parse(last)))
      forceCheckpoint("tbl")
      mutateTable(t) { tableDir =>
        val logDir = tableDir.resolve("_delta_log")
        val checkpoint = logDir.resolve("00000000000000000001.checkpoint.parquet")
        require(Files.isRegularFile(checkpoint), "Expected one classic checkpoint")
        val rewriteDir = logDir.resolve("timestamp-checkpoint-rewrite")
        val confKey = "spark.sql.parquet.outputTimestampType"
        val previous = spark.conf.get(confKey)
        try {
          spark.conf.set(confKey, encoding)
          val df = spark.read.parquet(checkpoint.toString)
          val stats = col("add.stats_parsed")
            .withField("minValues", col("add.stats_parsed.minValues")
              .withField("ts", lit(Timestamp.from(Instant.parse(lower)))))
            .withField("maxValues", col("add.stats_parsed.maxValues")
              .withField("ts", lit(Timestamp.from(Instant.parse(upper)))))
          // Disable the JSON fallback: these reads must consume parsed checkpoint stats.
          var add = col("add").withField("stats_parsed", stats)
          if (df.schema("add").dataType.asInstanceOf[StructType].fieldNames.contains("stats")) {
            add = add.withField("stats", lit(null).cast("string"))
          }
          df.withColumn("add", add).coalesce(1).write.parquet(rewriteDir.toString)
          val files = Files.list(rewriteDir)
          val part = try {
            val parts = files.iterator().asScala.filter(_.toString.endsWith(".parquet")).toSeq
            require(parts.size == 1)
            parts.head
          } finally files.close()
          Files.move(part, checkpoint, StandardCopyOption.REPLACE_EXISTING)
        } finally {
          spark.conf.set(confKey, previous)
          if (Files.exists(rewriteDir)) FileUtils.deleteDirectory(rewriteDir.toFile)
        }
        checkCheckpoint(checkpoint, encoding, lower, upper)
        if (encoding == "INT96") {
          // Spark's writer only emits microsecond-aligned INT96. Preserve every checkpoint
          // field with the existing Parquet Group API, replacing just the two stats cells.
          rewriteInt96Bounds(checkpoint)
          checkCheckpoint(checkpoint, encoding, nanoLower, nanoUpper)
        }
        // Retain v1 JSON as a discovery/metadata tail alongside the checkpoint. Removing v0
        // prevents full JSON replay, so v1 and latest require checkpoint bootstrap.
        val files = Files.list(logDir)
        try files.iterator().asScala.filter { p =>
          p.getFileName.toString == "00000000000000000000.json" ||
            p.toString.endsWith(".crc")
        }.foreach(Files.delete(_)) finally files.close()
      }
      checkSpecs(t)
    }
  }

  private def rewriteInt96Bounds(checkpoint: Path): Unit = {
    import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._

    val conf = spark.sparkContext.hadoopConfiguration
    val path = new HadoopPath(checkpoint.toUri)
    val footer = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))
    val metadata = try footer.getFooter.getFileMetaData finally footer.close()
    val parquetSchema = metadata.getSchema
    val factory = new SimpleGroupFactory(parquetSchema)
    // 2024-01-01 is Julian day 2460311. These nanoseconds after midnight enclose the rows.
    val replacements = Map(
      "add.stats_parsed.minValues.ts" -> new NanoTime(2460311, 123455111L).toBinary,
      "add.stats_parsed.maxValues.ts" -> new NanoTime(2460311, 123789999L).toBinary)

    def copyGroup(source: Group, target: Group, prefix: String): Unit = {
      for (i <- 0 until source.getType.getFieldCount) {
        val field = source.getType.getType(i)
        val fieldPath = if (prefix.isEmpty) field.getName else s"$prefix.${field.getName}"
        for (j <- 0 until source.getFieldRepetitionCount(i)) {
          if (!field.isPrimitive) {
            copyGroup(source.getGroup(i, j), target.addGroup(i), fieldPath)
          } else field.asPrimitiveType().getPrimitiveTypeName match {
            case BOOLEAN => target.add(i, source.getBoolean(i, j))
            case INT32 => target.add(i, source.getInteger(i, j))
            case INT64 => target.add(i, source.getLong(i, j))
            case FLOAT => target.add(i, source.getFloat(i, j))
            case DOUBLE => target.add(i, source.getDouble(i, j))
            case BINARY | FIXED_LEN_BYTE_ARRAY => target.add(i, source.getBinary(i, j))
            case INT96 => target.add(i, replacements.getOrElse(fieldPath, source.getInt96(i, j)))
          }
        }
      }
    }

    val temporary = checkpoint.resolveSibling("timestamp-int96-rewrite.parquet")
    try {
      val reader = ParquetReader.builder[Group](new GroupReadSupport(), path).withConf(conf).build()
      try {
        val writer = ExampleParquetWriter.builder(new HadoopPath(temporary.toUri))
          .withConf(conf).withType(parquetSchema)
          .withExtraMetaData(metadata.getKeyValueMetaData).build()
        try {
          var source = reader.read()
          while (source != null) {
            val target = factory.newGroup()
            copyGroup(source, target, "")
            writer.write(target)
            source = reader.read()
          }
        } finally writer.close()
      } finally reader.close()
      Files.move(temporary, checkpoint, StandardCopyOption.REPLACE_EXISTING)
    } finally Files.deleteIfExists(temporary)
  }

  private def checkCheckpoint(
      checkpoint: Path, encoding: String, lower: String, upper: String): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration
    val path = new HadoopPath(checkpoint.toUri)
    val footer = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))
    try {
      val parquetSchema = footer.getFooter.getFileMetaData.getSchema
      for (bound <- Seq("minValues", "maxValues")) {
        val physical = parquetSchema.getType("add", "stats_parsed", bound, "ts")
          .asPrimitiveType()
        if (encoding == "INT96") {
          require(physical.getPrimitiveTypeName == PrimitiveType.PrimitiveTypeName.INT96)
          require(physical.getLogicalTypeAnnotation == null)
        } else {
          require(physical.getPrimitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64)
          val annotation = physical.getLogicalTypeAnnotation
            .asInstanceOf[LogicalTypeAnnotation.TimestampLogicalTypeAnnotation]
          val unit = if (encoding == "TIMESTAMP_MILLIS") {
            LogicalTypeAnnotation.TimeUnit.MILLIS
          } else LogicalTypeAnnotation.TimeUnit.MICROS
          require(annotation.getUnit == unit)
          require(annotation.isAdjustedToUTC)
        }
        println(s"Verified checkpoint add.stats_parsed.$bound.ts: $physical")
      }
    } finally footer.close()

    // Read the physical cells without Spark's timestamp conversion or Delta's stats parser.
    val reader = ParquetReader.builder[Group](new GroupReadSupport(), path).withConf(conf).build()
    var adds = 0
    try {
      var row = reader.read()
      while (row != null) {
        if (row.getFieldRepetitionCount("add") > 0) {
          adds += 1
          val add = row.getGroup("add", 0)
          if (add.getType.containsField("stats")) require(add.getFieldRepetitionCount("stats") == 0)
          val stats = add.getGroup("stats_parsed", 0)
          require(stats.getLong("numRecords", 0) == 3)
          for ((bound, value) <- Seq("minValues" -> lower, "maxValues" -> upper)) {
            val cell = stats.getGroup(bound, 0)
            val instant = Instant.parse(value)
            if (encoding == "INT96") {
              val bytes = ByteBuffer.wrap(cell.getInt96("ts", 0).getBytes)
                .order(ByteOrder.LITTLE_ENDIAN)
              val nanosOfDay = bytes.getLong()
              val julianDay = bytes.getInt()
              require(julianDay == 2440588 + Math.floorDiv(instant.getEpochSecond, 86400L))
              require(nanosOfDay == Math.floorMod(instant.getEpochSecond, 86400L) *
                1000000000L + instant.getNano)
              // Check the aligned intermediate separately from the true nanos replacement.
              require(nanosOfDay % 1000 == instant.getNano % 1000)
              require(nanosOfDay % 1000000 != 0)
              println(s"Verified raw INT96 $bound: day=$julianDay nanosOfDay=$nanosOfDay")
            } else {
              val units = if (encoding == "TIMESTAMP_MILLIS") 1000L else 1000000L
              val expected = instant.getEpochSecond * units +
                instant.getNano / (1000000000L / units)
              require(cell.getLong("ts", 0) == expected)
              println(s"Verified raw $encoding $bound: $expected")
            }
          }
        }
        row = reader.read()
      }
    } finally reader.close()
    require(adds == 1)
  }

  // Each data boundary is crossed with every temporal partition boundary.
  // SQL fixture literals and the typed row oracle are deliberately declared separately.
  private val epochValues = Seq(
    (1, "1969-12-31T23:59:59.998999", "1969-12-31", -1001L),
    (2, "1969-12-31T23:59:59.999000", "1969-12-31", -1000L),
    (3, "1969-12-31T23:59:59.999001", "1969-12-31", -999L),
    (4, "1969-12-31T23:59:59.999999", "1969-12-31", -1L),
    (5, "1970-01-01T00:00:00.000000", "1970-01-01", 0L),
    (6, "1970-01-01T00:00:00.000001", "1970-01-01", 1L),
    (7, "1970-01-01T00:00:00.001000", "1970-01-02", 1000L))

  private val epochSchema = new StructType().add("id", IntegerType)
    .add("ts", TimestampType).add("ntz", org.apache.spark.sql.types.TimestampNTZType)
    .add("d", org.apache.spark.sql.types.DateType).add("p_ts", TimestampType)
    .add("p_ntz", org.apache.spark.sql.types.TimestampNTZType)
    .add("p_d", org.apache.spark.sql.types.DateType)

  private def epochRows: Seq[Row] = {
    val data = epochValues ++ epochValues.filter(_._1 == 4)
    for ((id, wall, day, _) <- data; (_, pWall, pDay, _) <- epochValues) yield {
      Row(id, Timestamp.from(Instant.parse(wall + "Z")), java.time.LocalDateTime.parse(wall),
        java.sql.Date.valueOf(day), Timestamp.from(Instant.parse(pWall + "Z")),
        java.time.LocalDateTime.parse(pWall), java.sql.Date.valueOf(pDay))
    }
  }

  private def epochPartitionStrings: Set[Map[String, String]] = epochValues.map {
    case (_, wall, day, _) =>
      // NTZ uses wall-clock strings; TIMESTAMP uses UTC ISO with six fractional digits.
      val partition = wall.replace('T', ' ').replaceAll("0+$", "").stripSuffix(".")
      Map("p_ts" -> (wall + "Z"), "p_ntz" -> partition, "p_d" -> day)
  }.toSet

  private def createEpochTable(): TableHandle = {
    require(spark.conf.get("spark.sql.session.timeZone") == "UTC")
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP, ntz TIMESTAMP_NTZ, d DATE,
      p_ts TIMESTAMP, p_ntz TIMESTAMP_NTZ, p_d DATE) USING delta
      PARTITIONED BY (p_ts, p_ntz, p_d) TBLPROPERTIES (
      'delta.checkpointPolicy' = 'classic',
      'delta.checkpoint.writeStatsAsStruct' = 'true',
      'delta.checkpoint.writeStatsAsJson' = 'false')""")
    val fixtures = Seq(
      (1, "1969-12-31 23:59:59.998999", "1969-12-31"),
      (2, "1969-12-31 23:59:59.999", "1969-12-31"),
      (3, "1969-12-31 23:59:59.999001", "1969-12-31"),
      (4, "1969-12-31 23:59:59.999999", "1969-12-31"),
      (5, "1970-01-01 00:00:00", "1970-01-01"),
      (6, "1970-01-01 00:00:00.000001", "1970-01-01"),
      (7, "1970-01-01 00:00:00.001", "1970-01-02"))
    val partitions = fixtures.map { case (_, ts, day) =>
      s"(TIMESTAMP '$ts', TIMESTAMP_NTZ '$ts', DATE '$day')"
    }.mkString(",")
    for ((id, ts, day) <- fixtures) {
      val data = s"($id, TIMESTAMP '$ts', TIMESTAMP_NTZ '$ts', DATE '$day')"
      val rows = if (id == 4) s"$data,$data" else data
      sql(s"""INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM
        (VALUES $rows) AS data(id, ts, ntz, d) CROSS JOIN
        (VALUES $partitions) AS parts(p_ts, p_ntz, p_d)""")
    }
    val t = registerTable("tbl")
    for (v <- 1L to 7L) {
      val adds = CommitLog.read(t.sourcePath, v).collect { case a: AddFile => a }
      require(adds.size == 7, "Each boundary must have one file per partition")
      require(adds.map(_.partitionValues).toSet == epochPartitionStrings)
    }
    t
  }

  for (parsed <- Seq(false, true)) {
    val name = if (parsed) "negative_epoch_parsed_stats_partitions"
      else "negative_epoch_json_stats_partitions"
    test(name) {
      val t = createEpochTable()
      if (parsed) forceCheckpoint("tbl")
      if (!parsed) {
        for ((id, wall, day, _) <- epochValues) {
          modifyCommitActions(t, version = id.toLong) { actions =>
            actions.map {
              case a: AddFile =>
                val stats = JsonUtil.mapper.readTree(a.stats.get)
                for (bound <- Seq("minValues", "maxValues")) {
                  val values = stats.get(bound)
                    .asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
                  values.put("ts", wall + "Z")
                  values.put("ntz", wall)
                  values.put("d", day)
                }
                a.copy(stats = Some(JsonUtil.mapper.writeValueAsString(stats)))
              case other => other
            }
          }
        }
      }
      mutateTable(t) { dir =>
        if (parsed) {
          val checkpoint = dir.resolve("_delta_log/00000000000000000007.checkpoint.parquet")
          val rewrite = dir.resolve("_delta_log/epoch-checkpoint-rewrite")
          val confKey = "spark.sql.parquet.outputTimestampType"
          val previous = spark.conf.get(confKey)
          try {
            spark.conf.set(confKey, "TIMESTAMP_MICROS")
            val df = spark.read.parquet(checkpoint.toString)
            var stats = col("add.stats_parsed")
            for (bound <- Seq("minValues", "maxValues")) {
              var ts = col(s"add.stats_parsed.$bound.ts")
              var ntz = col(s"add.stats_parsed.$bound.ntz")
              for ((id, wall, _, _) <- epochValues) {
                val matches = col("add.stats_parsed.minValues.id") === lit(id)
                ts = org.apache.spark.sql.functions.when(matches,
                  lit(Timestamp.from(Instant.parse(wall + "Z")))).otherwise(ts)
                ntz = org.apache.spark.sql.functions.when(matches,
                  lit(java.time.LocalDateTime.parse(wall))).otherwise(ntz)
              }
              stats = stats.withField(bound, col(s"add.stats_parsed.$bound")
                .withField("ts", ts).withField("ntz", ntz))
            }
            var add = col("add").withField("stats_parsed", stats)
            if (df.schema("add").dataType.asInstanceOf[StructType].fieldNames.contains("stats")) {
              add = add.withField("stats", lit(null).cast("string"))
            }
            df.withColumn("add", add).coalesce(1).write.parquet(rewrite.toString)
            val files = Files.list(rewrite)
            val part = try files.iterator().asScala.filter(_.toString.endsWith(".parquet"))
              .toSeq match {
                case Seq(single) => single
                case other => fail(s"Expected one rewritten checkpoint: $other")
              } finally files.close()
            Files.move(part, checkpoint, StandardCopyOption.REPLACE_EXISTING)
          } finally {
            spark.conf.set(confKey, previous)
            if (Files.exists(rewrite)) FileUtils.deleteDirectory(rewrite.toFile)
          }
          checkEpochCheckpoint(checkpoint)
        } else {
          for ((id, wall, day, _) <- epochValues) {
            val adds = CommitLog.read(dir, id.toLong).collect { case a: AddFile => a }
            require(adds.size == 7)
            require(adds.map(_.partitionValues).toSet == epochPartitionStrings)
            for (a <- adds) {
              val stats = JsonUtil.mapper.readTree(a.stats.get)
              require(stats.get("numRecords").asLong() == (if (id == 4) 2 else 1))
              for (bound <- Seq("minValues", "maxValues")) {
                require(stats.at(s"/$bound/ts").asText() == wall + "Z")
                require(stats.at(s"/$bound/ntz").asText() == wall)
                require(stats.at(s"/$bound/d").asText() == day)
                require(stats.at(s"/$bound/id").asInt() == id)
              }
            }
          }
          println("Verified exact JSON stats and partition strings for all 49 boundary files")
        }
        val files = Files.list(dir.resolve("_delta_log"))
        try files.iterator().asScala.filter { p =>
          val file = p.getFileName.toString
          file.endsWith(".crc") || (parsed && file.endsWith(".json") &&
            file != "00000000000000000007.json")
        }.foreach(Files.delete(_)) finally files.close()
        // v7 JSON is retained only for same-version lookup. There is no replay tail after v7.
        // v0-v6 are absent, so the checkpoint is necessary for both latest and version 7.
      }
      checkEpochSpecs(t, parsed)
    }
  }

  private def checkEpochCheckpoint(checkpoint: Path): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration
    val path = new HadoopPath(checkpoint.toUri)
    val footer = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))
    try {
      val physical = footer.getFooter.getFileMetaData.getSchema
      for ((prefix, fields) <- Seq(
          "stats_parsed.minValues" -> Seq("ts", "ntz", "d"),
          "stats_parsed.maxValues" -> Seq("ts", "ntz", "d"),
          "partitionValues_parsed" -> Seq("p_ts", "p_ntz", "p_d")); field <- fields) {
        val cell = physical.getType((Seq("add") ++ prefix.split("\\.") :+ field): _*)
          .asPrimitiveType()
        if (field.endsWith("d")) {
          require(cell.getPrimitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32)
          require(cell.getLogicalTypeAnnotation == LogicalTypeAnnotation.dateType())
        } else {
          require(cell.getPrimitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64)
          val annotation = cell.getLogicalTypeAnnotation
            .asInstanceOf[LogicalTypeAnnotation.TimestampLogicalTypeAnnotation]
          require(annotation.getUnit == LogicalTypeAnnotation.TimeUnit.MICROS)
          require(annotation.isAdjustedToUTC == field.endsWith("ts"))
        }
        println(s"Verified near-epoch checkpoint $prefix.$field: $cell")
      }
    } finally footer.close()
    val reader = ParquetReader.builder[Group](new GroupReadSupport(), path).withConf(conf).build()
    var pairs = Vector.empty[(Int, Map[String, String])]
    try {
      var row = reader.read()
      while (row != null) {
        if (row.getFieldRepetitionCount("add") > 0) {
          val add = row.getGroup("add", 0)
          if (add.getType.containsField("stats")) require(add.getFieldRepetitionCount("stats") == 0)
          val stats = add.getGroup("stats_parsed", 0)
          val id = stats.getGroup("minValues", 0).getInteger("id", 0)
          val (_, _, day, micros) = epochValues.find(_._1 == id).get
          require(stats.getLong("numRecords", 0) == (if (id == 4) 2 else 1))
          for (bound <- Seq("minValues", "maxValues")) {
            val values = stats.getGroup(bound, 0)
            require(values.getInteger("id", 0) == id)
            require(values.getLong("ts", 0) == micros)
            require(values.getLong("ntz", 0) == micros)
            require(values.getInteger("d", 0) == java.time.LocalDate.parse(day).toEpochDay)
          }
          val raw = add.getGroup("partitionValues", 0)
          val partition = (0 until raw.getFieldRepetitionCount("key_value")).map { i =>
            val entry = raw.getGroup("key_value", i)
            entry.getString("key", 0) -> entry.getString("value", 0)
          }.toMap
          require(epochPartitionStrings.contains(partition))
          val parsed = add.getGroup("partitionValues_parsed", 0)
          val (_, _, pDay, pMicros) = epochValues.find { case (_, wall, _, _) =>
            java.time.LocalDateTime.parse(wall) ==
              java.time.LocalDateTime.parse(partition("p_ntz").replace(' ', 'T'))
          }.get
          require(parsed.getLong("p_ts", 0) == pMicros)
          require(parsed.getLong("p_ntz", 0) == pMicros)
          require(parsed.getInteger("p_d", 0) == java.time.LocalDate.parse(pDay).toEpochDay)
          pairs :+= id -> partition
        }
        row = reader.read()
      }
    } finally reader.close()
    val expectedPairs = for ((id, _, _, _) <- epochValues; p <- epochPartitionStrings) yield id -> p
    require(pairs.size == 49 && pairs.toSet == expectedPairs.toSet)
    println("Verified 49 physical stats/partition pairs: micros -1001,-1000,-999,-1,0,1,1000")
  }

  private def checkEpochSpecs(t: TableHandle, parsed: Boolean): Unit = {
    val workload = WorkloadContext.current.workloadName
    val tableDir = outputDir.resolve(workload)
    val all = epochRows
    def read(name: String, predicate: String, rows: Seq[Row], version: java.lang.Long = null)
        : Unit = {
      readSpec(t, predicate = predicate, version = version, name = Some(name)).assert { spec =>
        spec.expectation match {
          case Succeeded(result) => require(result.rowCount == rows.size)
          case other => fail(s"$name must succeed: $other")
        }
        val expected = spark.createDataFrame(rows.asJava, epochSchema)
        val captured = tableDir.resolve("expected").resolve(s"${workload}_$name")
          .resolve("expected_data")
        RowComparison.assertRowsEqual(expected, spark.read.parquet(captured.toString), name)
      }
    }
    read("all", null, all)
    read("all_v7", null, all, 7L)
    for ((field, index, kind) <- Seq(("ts", 1, "TIMESTAMP"), ("ntz", 2, "TIMESTAMP_NTZ"),
        ("p_ts", 4, "TIMESTAMP"), ("p_ntz", 5, "TIMESTAMP_NTZ"))) {
      for ((id, wall, _, _) <- epochValues) {
        val expected = if (kind == "TIMESTAMP") Timestamp.from(Instant.parse(wall + "Z"))
          else java.time.LocalDateTime.parse(wall)
        read(s"${field}_eq_$id", s"$field = $kind '${wall.replace('T', ' ')}'",
          all.filter(_.get(index) == expected))
      }
      read(s"${field}_negative", s"$field < $kind '1970-01-01 00:00:00'",
        all.filter { r =>
          if (kind == "TIMESTAMP") r.getTimestamp(index).before(Timestamp.from(Instant.EPOCH))
          else r.getAs[java.time.LocalDateTime](index)
            .isBefore(java.time.LocalDateTime.of(1970, 1, 1, 0, 0))
        })
    }
    for ((field, index) <- Seq("d" -> 3, "p_d" -> 6);
        day <- Seq("1969-12-31", "1970-01-01", "1970-01-02")) {
      read(s"${field}_$day", s"$field = DATE '$day'",
        all.filter(_.getDate(index) == java.sql.Date.valueOf(day)))
    }
    read("cross_negative_hits", "ts = TIMESTAMP '1969-12-31 23:59:59.999999' AND " +
      "p_ntz = TIMESTAMP_NTZ '1969-12-31 23:59:59.999' AND p_d < DATE '1970-01-01'",
      all.filter(r => r.getInt(0) == 4 && r.getTimestamp(4) ==
        Timestamp.from(Instant.parse("1969-12-31T23:59:59.999Z"))))
    read("outside_stats", "ts < TIMESTAMP '1969-12-31 23:59:59.998999' OR " +
      "ntz > TIMESTAMP_NTZ '1970-01-01 00:00:00.001'", Seq.empty)
    read("outside_partitions", "p_ts < TIMESTAMP '1969-12-31 23:59:59.998999' OR " +
      "p_ntz > TIMESTAMP_NTZ '1970-01-01 00:00:00.001'", Seq.empty)
    for (version <- Seq[java.lang.Long](null, 7L)) {
      snapshotSpec(t, version = version).assert { spec =>
        spec.expectation match {
          case Succeeded(result) =>
            require(DataType.fromJson(result.metadata.schemaString) == epochSchema)
            require(result.metadata.partitionColumns == Seq("p_ts", "p_ntz", "p_d"))
            require(result.metadata.format.provider == "parquet")
            require(result.metadata.format.options.isEmpty)
            require(result.metadata.configuration("delta.checkpointPolicy") == "classic")
            require(result.protocol.minReaderVersion == 3)
            require(result.protocol.minWriterVersion == 7)
            require(result.protocol.readerFeatures.get.contains("timestampNtz"))
            require(result.protocol.writerFeatures.get.contains("timestampNtz"))
          case other => fail(s"Near-epoch snapshot must succeed (parsed=$parsed): $other")
        }
      }
    }
  }

  // Statistics and partition cases share the absolute-instant fixture and oracle.
  private val mismatchSchema = new StructType().add("id", IntegerType)
    .add("ts", TimestampType).add("ntz", org.apache.spark.sql.types.TimestampNTZType)
  private val mismatchZones = Seq(
    ("writer_utc_reader_non_utc", "UTC", "America/Los_Angeles"),
    ("writer_non_utc_reader_utc", "America/Los_Angeles", "UTC"))

  // Offsets distinguish both occurrences of 01:30 at the autumn overlap. The unqualified
  // controls deliberately produce different instants in the two writer-zone fixtures.
  private val mismatchSql = Seq(
    (1, "2024-03-10 01:59:59.123456-08:00", "2024-03-10 01:59:59.123456"),
    (2, "2024-03-10 03:00:00.123456-07:00", "2024-03-10 03:00:00.123456"),
    (3, "2024-11-03 01:30:00.123456-07:00", "2024-11-03 01:30:00.123456"),
    (4, "2024-11-03 01:30:00.123456-08:00", "2024-11-03 01:30:00.123456"),
    (5, "2024-01-15 12:00:00.654321", "2024-01-15 12:00:00.654321"),
    (6, "2024-03-10 02:30:00.123456", "2024-03-10 02:30:00.123456"),
    (7, "2024-11-03 01:30:00.123456", "2024-11-03 01:30:00.123456"))

  // This oracle does not parse fixture SQL or read Delta. NTZ retains the written wall clock,
  // including the nonexistent LA 02:30. TIMESTAMP instead denotes an absolute UTC instant.
  private def mismatchRows(writerZone: String): Seq[Row] = {
    val controls = if (writerZone == "UTC") Seq(
      "2024-01-15T12:00:00.654321Z", "2024-03-10T02:30:00.123456Z",
      "2024-11-03T01:30:00.123456Z") else Seq(
      "2024-01-15T20:00:00.654321Z", "2024-03-10T10:30:00.123456Z",
      "2024-11-03T08:30:00.123456Z")
    val instants = Seq(
      "2024-03-10T09:59:59.123456Z", "2024-03-10T10:00:00.123456Z",
      "2024-11-03T08:30:00.123456Z", "2024-11-03T09:30:00.123456Z") ++ controls
    val walls = Seq(
      "2024-03-10T01:59:59.123456", "2024-03-10T03:00:00.123456",
      "2024-11-03T01:30:00.123456", "2024-11-03T01:30:00.123456",
      "2024-01-15T12:00:00.654321", "2024-03-10T02:30:00.123456",
      "2024-11-03T01:30:00.123456")
    val rows = instants.zip(walls).zipWithIndex.map { case ((instant, wall), i) =>
      Row(i + 1, Timestamp.from(Instant.parse(instant)), java.time.LocalDateTime.parse(wall))
    }
    rows ++ rows.filter(_.getInt(0) == 3)
  }

  private def mismatchMicros(instant: Instant): Long =
    instant.getEpochSecond * 1000000L + instant.getNano / 1000

  // Spark to_json's default timestamp pattern serializes both bounds at millisecond
  // precision. Check that physical encoding independently; keep microsecond row expectations.
  private def mismatchJsonInstant(instant: Instant): Instant =
    instant.truncatedTo(java.time.temporal.ChronoUnit.MILLIS)

  private def createMismatchTable(writerZone: String, partitioned: Boolean): TableHandle = {
    spark.conf.set("spark.sql.session.timeZone", writerZone)
    val partition = if (partitioned) "PARTITIONED BY (ts)" else ""
    sql(s"""CREATE TABLE tbl (id INT, ts TIMESTAMP, ntz TIMESTAMP_NTZ) USING delta
      $partition TBLPROPERTIES ('delta.checkpointPolicy' = 'classic',
      'delta.checkpointInterval' = '100', 'delta.checkpoint.writeStatsAsStruct' = 'true',
      'delta.checkpoint.writeStatsAsJson' = 'false')""")
    for ((id, instant, wall) <- mismatchSql) {
      val row = s"($id, TIMESTAMP '$instant', TIMESTAMP_NTZ '$wall')"
      val values = if (id == 3) s"$row, $row" else row
      sql(s"INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES $values AS rows(id, ts, ntz)")
    }
    val table = registerTable("tbl")
    val rows = mismatchRows(writerZone)
    for (version <- 1L to 7L) {
      val adds = CommitLog.read(table.sourcePath, version).collect { case a: AddFile => a }
      require(adds.size == 1, "Each input batch must produce exactly one file")
      val add = adds.head
      val row = rows.find(_.getInt(0) == version.toInt).get
      val stats = JsonUtil.mapper.readTree(add.stats.get)
      require(stats.get("numRecords").asLong() == (if (version == 3) 2 else 1))
      val expectedPartition = if (partitioned) {
        Map("ts" -> row.getTimestamp(1).toInstant.toString)
      } else Map.empty[String, String]
      if (partitioned && writerZone == "America/Los_Angeles" && version == 4 &&
          add.partitionValues != expectedPartition) {
        // The producer reparses an offset-free directory name at the overlap's earlier offset.
        // Preserve that limitation, then construct the intended AddFile in the copied fixture.
        require(add.partitionValues == Map("ts" -> "2024-11-03T08:30:00.123456Z"))
        println(s"Observed producer overlap loss: expected=$expectedPartition " +
          s"actual=${add.partitionValues} action=${add.toJson}")
      } else require(add.partitionValues == expectedPartition)
      for (bound <- Seq("minValues", "maxValues")) {
        val fields = stats.get(bound)
        require(fields.get("id").asInt() == version.toInt)
        if (partitioned) require(!fields.has("ts")) else {
          val raw = fields.get("ts").asText()
          val parsed = java.time.OffsetDateTime.parse(raw)
          val instant = row.getTimestamp(1).toInstant
          require(parsed.toInstant == mismatchJsonInstant(instant))
          require(parsed.getOffset == instant.atZone(java.time.ZoneId.of(writerZone)).getOffset)
        }
        val ntz = row.getAs[java.time.LocalDateTime](2).toInstant(java.time.ZoneOffset.UTC)
        val rawNtz = java.time.LocalDateTime.parse(fields.get("ntz").asText())
        require(rawNtz.toInstant(java.time.ZoneOffset.UTC) == mismatchJsonInstant(ntz))
      }
      println(s"Verified writer JSON zone=$writerZone partitioned=$partitioned " +
        s"id=$version stats=${add.stats.get} partitions=${add.partitionValues}")
    }
    println(s"Verified writer table zone=${spark.conf.get("spark.sql.session.timeZone")} " +
      s"partitioned=$partitioned rows=8 files=7")
    table
  }

  private def prepareMismatchCapture(
      table: TableHandle, writerZone: String, readerZone: String,
      partitioned: Boolean, parsed: Boolean): Unit = {
    require(writerZone != readerZone)
    require(spark.conf.get("spark.sql.session.timeZone") == writerZone)
    if (partitioned && writerZone == "America/Los_Angeles") {
      val later = mismatchRows(writerZone).find(_.getInt(0) == 4).get.getTimestamp(1).toInstant
      modifyCommitActions(table, version = 4) { actions =>
        actions.map {
          case add: AddFile => add.copy(partitionValues = Map("ts" -> later.toString))
          case other => other
        }
      }
    }
    if (parsed) {
      val encoding = spark.conf.get("spark.sql.parquet.outputTimestampType")
      try {
        spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        forceCheckpoint("tbl")
      } finally spark.conf.set("spark.sql.parquet.outputTimestampType", encoding)
    }
    mutateTable(table) { dir =>
      require(spark.conf.get("spark.sql.session.timeZone") == readerZone)
      val logDir = dir.resolve("_delta_log")
      if (parsed) {
        val checkpoint = logDir.resolve("00000000000000000007.checkpoint.parquet")
        if (partitioned && writerZone == "America/Los_Angeles") {
          setMismatchOverlapPartition(checkpoint)
        }
        checkMismatchCheckpoint(checkpoint, writerZone, partitioned)
      }
      val files = Files.list(logDir)
      try files.iterator().asScala.filter { p =>
        val file = p.getFileName.toString
        file.endsWith(".crc") || (parsed && file.endsWith(".json") &&
          file != "00000000000000000007.json")
      }.foreach(Files.delete(_)) finally files.close()
      // Same-version v7 JSON is retained solely for version lookup. No replay tail exists;
      // v0-v6 and CRC shortcuts are absent, so the checkpoint is required, not checkpoint-only.
      println(s"Verified pre-capture reader=$readerZone writer=$writerZone " +
        s"partitioned=$partitioned parsed=$parsed")
    }
    // WorkloadTestSuite captures after the body. Deliberately leave this reader zone active.
    spark.conf.set("spark.sql.session.timeZone", readerZone)
  }

  // DAT supports a synthetic, correct overlap partition even when the SQL writer loses the
  // later offset while reparsing a partition directory. Preserve its unmodified checkpoint;
  // then change only the intended id=4 AddFile's raw and parsed partition cells in the copy.
  private def setMismatchOverlapPartition(checkpoint: Path): Unit = {
    val workload = WorkloadContext.current.workloadName
    val original = outputDir.resolveSibling("producer-checkpoints").resolve(workload)
    Files.createDirectories(original)
    Files.copy(checkpoint, original.resolve(checkpoint.getFileName),
      StandardCopyOption.REPLACE_EXISTING)
    val instant = Instant.parse("2024-11-03T09:30:00.123456Z")
    val temporary = checkpoint.resolveSibling("overlap-partition-rewrite")
    val confKey = "spark.sql.parquet.outputTimestampType"
    val previous = spark.conf.get(confKey)
    try {
      spark.conf.set(confKey, "TIMESTAMP_MICROS")
      val df = spark.read.parquet(checkpoint.toString)
      val later = col("add.stats_parsed.minValues.id") === lit(4)
      val raw = org.apache.spark.sql.functions.when(later,
        org.apache.spark.sql.functions.map(lit("ts"), lit(instant.toString)))
        .otherwise(col("add.partitionValues"))
      val parsed = col("add.partitionValues_parsed").withField("ts",
        org.apache.spark.sql.functions.when(later, lit(Timestamp.from(instant)))
          .otherwise(col("add.partitionValues_parsed.ts")))
      val add = col("add").withField("partitionValues", raw)
        .withField("partitionValues_parsed", parsed)
      df.withColumn("add", add).coalesce(1).write.parquet(temporary.toString)
      val files = Files.list(temporary)
      val part = try files.iterator().asScala.filter(_.toString.endsWith(".parquet"))
        .toSeq match {
          case Seq(single) => single
          case other => fail(s"Expected one overlap checkpoint part: $other")
        } finally files.close()
      Files.move(part, checkpoint, StandardCopyOption.REPLACE_EXISTING)
    } finally {
      spark.conf.set(confKey, previous)
      if (Files.exists(temporary)) FileUtils.deleteDirectory(temporary.toFile)
    }
    println("Constructed later overlap partition fixture: id=4 " +
      "instant=2024-11-03T09:30:00.123456Z")
  }

  private def checkMismatchCheckpoint(
      checkpoint: Path, writerZone: String, partitioned: Boolean): Unit = {
    require(Files.isRegularFile(checkpoint))
    val conf = spark.sparkContext.hadoopConfiguration
    val path = new HadoopPath(checkpoint.toUri)
    val footer = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))
    try {
      val physical = footer.getFooter.getFileMetaData.getSchema
      val timestamps = if (partitioned) Seq("partitionValues_parsed" -> "ts")
        else Seq("stats_parsed.minValues" -> "ts", "stats_parsed.maxValues" -> "ts")
      for ((prefix, field) <- timestamps ++ Seq(
          "stats_parsed.minValues" -> "ntz", "stats_parsed.maxValues" -> "ntz")) {
        val cell = physical.getType((Seq("add") ++ prefix.split("\\.") :+ field): _*)
          .asPrimitiveType()
        require(cell.getPrimitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64)
        val annotation = cell.getLogicalTypeAnnotation
          .asInstanceOf[LogicalTypeAnnotation.TimestampLogicalTypeAnnotation]
        require(annotation.getUnit == LogicalTypeAnnotation.TimeUnit.MICROS)
        require(annotation.isAdjustedToUTC == (field == "ts"))
        println(s"Verified timezone checkpoint writer=$writerZone $prefix.$field: $cell")
      }
    } finally footer.close()
    val rows = mismatchRows(writerZone)
    val reader = ParquetReader.builder[Group](new GroupReadSupport(), path).withConf(conf).build()
    var ids = Vector.empty[Int]
    try {
      var record = reader.read()
      while (record != null) {
        if (record.getFieldRepetitionCount("add") > 0) {
          val add = record.getGroup("add", 0)
          if (add.getType.containsField("stats")) require(add.getFieldRepetitionCount("stats") == 0)
          val stats = add.getGroup("stats_parsed", 0)
          val id = stats.getGroup("minValues", 0).getInteger("id", 0)
          val row = rows.find(_.getInt(0) == id).get
          require(stats.getLong("numRecords", 0) == (if (id == 3) 2 else 1))
          val instant = row.getTimestamp(1).toInstant
          val ntz = row.getAs[java.time.LocalDateTime](2).toInstant(java.time.ZoneOffset.UTC)
          for (bound <- Seq("minValues", "maxValues")) {
            val values = stats.getGroup(bound, 0)
            require(values.getInteger("id", 0) == id)
            if (!partitioned) require(values.getLong("ts", 0) ==
              mismatchMicros(mismatchJsonInstant(instant)))
            require(values.getLong("ntz", 0) == mismatchMicros(mismatchJsonInstant(ntz)))
          }
          if (partitioned) {
            val partition = add.getGroup("partitionValues_parsed", 0)
            require(partition.getLong("ts", 0) == mismatchMicros(instant))
            val raw = add.getGroup("partitionValues", 0)
            require(raw.getFieldRepetitionCount("key_value") == 1)
            val entry = raw.getGroup("key_value", 0)
            require(entry.getString("key", 0) == "ts")
            require(entry.getString("value", 0) == instant.toString)
          }
          ids :+= id
          println(s"Verified timezone raw checkpoint writer=$writerZone partitioned=$partitioned " +
            s"id=$id instantMicros=${mismatchMicros(instant)} ntzMicros=${mismatchMicros(ntz)}")
        }
        record = reader.read()
      }
    } finally reader.close()
    require(ids.sorted == (1 to 7).toVector)
  }

  private def checkMismatchSpecs(
      table: TableHandle, writerZone: String, readerZone: String, partitioned: Boolean): Unit = {
    val workload = WorkloadContext.current.workloadName
    val tableDir = outputDir.resolve(workload)
    val all = mismatchRows(writerZone)
    def checkReaderZone(label: String): Unit = {
      require(spark.conf.get("spark.sql.session.timeZone") == readerZone)
      val probe = spark.sql("SELECT current_timezone(), " +
        "CAST(TIMESTAMP '2024-03-10 10:00:00.123456Z' AS STRING)").head()
      require(probe.getString(0) == readerZone)
      val wall = if (readerZone == "UTC") "2024-03-10 10:00:00.123456"
        else "2024-03-10 03:00:00.123456"
      require(probe.getString(1) == wall)
      println(s"Verified captured $workload/$label writer=$writerZone reader=$readerZone " +
        s"readerWall=$wall")
    }
    def read(label: String, predicate: String, rows: Seq[Row], version: java.lang.Long): Unit = {
      readSpec(table, predicate = predicate, version = version, name = Some(label)).assert { spec =>
        checkReaderZone(label)
        spec.expectation match {
          case Succeeded(result) => require(result.rowCount == rows.size)
          case other => fail(s"$label must succeed: $other")
        }
        val expected = spark.createDataFrame(rows.asJava, mismatchSchema)
        val captured = tableDir.resolve("expected").resolve(s"${workload}_$label")
          .resolve("expected_data")
        RowComparison.assertRowsEqual(expected, spark.read.parquet(captured.toString), label)
        if (label == "all_latest") {
          expected.coalesce(1).write.mode("overwrite")
            .parquet(outputDir.resolveSibling("oracle-parquet").resolve(workload).toString)
        }
      }
    }
    for (version <- Seq[java.lang.Long](null, 7L)) {
      val suffix = if (version == null) "latest" else "v7"
      read(s"all_$suffix", null, all, version)
      // Explicit offsets keep the emitted predicates portable to consumers with another zone.
      for (row <- all.groupBy(_.getInt(0)).values.map(_.head).toSeq.sortBy(_.getInt(0))) {
        val instant = row.getTimestamp(1)
        read(s"eq_${row.getInt(0)}_$suffix", s"ts = TIMESTAMP '${instant.toInstant}'",
          all.filter(_.getTimestamp(1) == instant), version)
      }
      read(s"spring_range_$suffix", "ts >= TIMESTAMP '2024-03-10T09:59:59.123456Z' AND " +
        "ts <= TIMESTAMP '2024-03-10T10:00:00.123456Z'",
        all.filter(r => Set(1, 2).contains(r.getInt(0))), version)
      read(s"overlap_range_$suffix", "ts >= TIMESTAMP '2024-11-03T08:30:00.123456Z' AND " +
        "ts <= TIMESTAMP '2024-11-03T09:30:00.123456Z'",
        all.filter(r => Set(3, 4).contains(r.getInt(0)) ||
          (writerZone != "UTC" && r.getInt(0) == 7)), version)
      read(s"outside_$suffix", "ts < TIMESTAMP '2024-01-01T00:00:00Z'", Seq.empty, version)
      read(s"ntz_gap_$suffix", "ntz = TIMESTAMP_NTZ '2024-03-10 02:30:00.123456'",
        all.filter(_.getInt(0) == 6), version)
      snapshotSpec(table, version = version).assert { spec =>
        checkReaderZone(s"snapshot_$suffix")
        spec.expectation match {
          case Succeeded(result) =>
            require(DataType.fromJson(result.metadata.schemaString) == mismatchSchema)
            require(result.metadata.partitionColumns == (if (partitioned) Seq("ts") else Seq.empty))
            require(result.metadata.format.provider == "parquet")
            require(result.metadata.format.options.isEmpty)
            require(result.metadata.configuration("delta.checkpointPolicy") == "classic")
            require(result.metadata.configuration("delta.checkpoint.writeStatsAsStruct") == "true")
            require(result.metadata.configuration("delta.checkpoint.writeStatsAsJson") == "false")
            require(result.protocol.minReaderVersion == 3 && result.protocol.minWriterVersion == 7)
            require(result.protocol.readerFeatures.get.contains("timestampNtz"))
            require(result.protocol.writerFeatures.get.contains("timestampNtz"))
          case other => fail(s"Timezone snapshot must succeed: $other")
        }
      }
    }
  }

  for ((direction, writer, reader) <- mismatchZones; parsed <- Seq(false, true)) {
    val representation = if (parsed) "checkpoint" else "json"
    test(s"stats_timezone_${direction}_$representation") {
      val table = createMismatchTable(writer, partitioned = false)
      prepareMismatchCapture(table, writer, reader, partitioned = false, parsed = parsed)
      checkMismatchSpecs(table, writer, reader, partitioned = false)
    }
  }

  // Reuse the timezone fixture and check TIMESTAMP partition reconstruction.
  for ((direction, writer, reader) <- mismatchZones; parsed <- Seq(false, true)) {
    val representation = if (parsed) "checkpoint" else "json"
    test(s"partitions_timezone_${direction}_$representation") {
      val table = createMismatchTable(writer, partitioned = true)
      prepareMismatchCapture(table, writer, reader, partitioned = true, parsed = parsed)
      checkMismatchSpecs(table, writer, reader, partitioned = true)
    }
  }

  // Later portable cases share only ordinary read/schema assertions; row oracles stay literal.
  private def readinessRead(
      table: TableHandle, name: String, expectedSchema: StructType, rows: Seq[Row],
      predicate: String = null, version: java.lang.Long = null): Unit = {
    val workload = WorkloadContext.current.workloadName
    readSpec(table, predicate = predicate, version = version, name = Some(name)).assert { spec =>
      spec.expectation match {
        case Succeeded(result) => require(result.rowCount == rows.size)
        case other => fail(s"$name must succeed against the independent oracle: $other")
      }
      val expected = spark.createDataFrame(rows.asJava, expectedSchema)
      val captured = outputDir.resolve(workload).resolve("expected")
        .resolve(s"${workload}_$name").resolve("expected_data")
      RowComparison.assertRowsEqual(expected, spark.read.parquet(captured.toString), name)
      if (name == "all") {
        expected.coalesce(1).write.mode("overwrite")
          .parquet(outputDir.resolveSibling("oracle-parquet").resolve(workload).toString)
      }
      println(s"Verified independent literal rows/schema: $workload/$name rows=${rows.size}")
    }
  }

  private def readinessSnapshot(
      table: TableHandle, expectedSchema: StructType, partitions: Seq[String] = Seq.empty,
      version: java.lang.Long = null): Unit = {
    snapshotSpec(table, version = version).assert { spec =>
      spec.expectation match {
        case Succeeded(result) =>
          require(DataType.fromJson(result.metadata.schemaString) == expectedSchema)
          require(result.metadata.partitionColumns == partitions)
          require(result.metadata.format.provider == "parquet")
          require(result.metadata.format.options.isEmpty)
          val ntz = expectedSchema.fields.exists(_.dataType ==
            org.apache.spark.sql.types.TimestampNTZType)
          require(result.protocol.minReaderVersion == (if (ntz) 3 else 1))
          require(result.protocol.minWriterVersion == (if (ntz) 7 else 2))
        case other => fail(s"Snapshot must succeed against independent metadata: $other")
      }
    }
  }

  private def readinessPruneHistory(dir: Path, version: Long): Unit = {
    val log = dir.resolve("_delta_log")
    val files = Files.list(log)
    try files.iterator().asScala.filter { p =>
      val file = p.getFileName.toString
      file.endsWith(".crc") || (file.endsWith(".json") && file != f"$version%020d.json")
    }.foreach(Files.delete(_)) finally files.close()
    // The same-version JSON supports requested-version lookup only. Earlier history is absent,
    // so checkpoint reconstruction is required. No replay tail or checkpoint-only claim.
  }

  // Unannotated INT32/INT64 parsed cells are distinct from normal DATE/TIMESTAMP
  // Parquet encodings. Keep correct JSON stats too: Spark rejects several parsed-only forms.
  // Only INT32 as DATE, the DATE32 physical width, removes every JSON stats copy so that the
  // parsed day is the only statistic. The other forms are not claimed as parsed-only routes.
  for ((kind, sqlType, valueType, literal, value, integerValue) <- Seq(
      ("timestamp", "TIMESTAMP", TimestampType,
        "TIMESTAMP '1970-01-01T00:00:01Z'", Timestamp.from(Instant.ofEpochSecond(1)), 1000000L),
      ("timestamp_ntz", "TIMESTAMP_NTZ", org.apache.spark.sql.types.TimestampNTZType,
        "TIMESTAMP_NTZ '1970-01-01 00:00:01'",
        java.time.LocalDateTime.of(1970, 1, 1, 0, 0, 1), 1000000L),
      ("date", "DATE", org.apache.spark.sql.types.DateType,
        "DATE '1970-01-02'", java.sql.Date.valueOf("1970-01-02"), 1L));
      integerType <- Seq("int", "long")) {
    test(s"checkpoint_integer_${integerType}_as_$kind") {
      spark.conf.set("spark.sql.session.timeZone", "UTC")
      val expectedSchema = new StructType().add("id", IntegerType).add("v", valueType)
      val rows = Seq(Row(1, value), Row(2, value), Row(2, value))
      sql(s"""CREATE TABLE tbl (id INT, v $sqlType) USING delta TBLPROPERTIES (
        'delta.checkpointPolicy' = 'classic', 'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.checkpoint.writeStatsAsJson' = 'true')""")
      sql(s"INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES " +
        s"(1, $literal), (2, $literal), (2, $literal) AS rows(id, v)")
      val table = registerTable("tbl")
      val parsedOnly = kind == "date" && integerType == "int"
      forceCheckpoint("tbl")
      if (parsedOnly) {
        modifyCommitActions(table, 1L) { actions =>
          actions.map { case add: AddFile => add.copy(stats = None); case other => other }
        }
      }
      mutateTable(table) { dir =>
        val checkpoint = dir.resolve("_delta_log/00000000000000000001.checkpoint.parquet")
        val temporary = dir.resolve("_delta_log/integer-stats-rewrite")
        val df = spark.read.parquet(checkpoint.toString)
        var stats = col("add.stats_parsed")
        for (bound <- Seq("minValues", "maxValues")) {
          stats = stats.withField(bound, col(s"add.stats_parsed.$bound")
            .withField("v", lit(integerValue).cast(integerType)))
        }
        var add = col("add").withField("stats_parsed", stats)
        if (parsedOnly) add = add.withField("stats", lit(null).cast("string"))
        df.withColumn("add", add).coalesce(1).write.parquet(temporary.toString)
        val parts = Files.list(temporary)
        val part = try parts.iterator().asScala.filter(_.toString.endsWith(".parquet"))
          .toSeq match {
            case Seq(single) => single
            case other => fail(s"Expected one rewritten integer stats checkpoint: $other")
          } finally parts.close()
        Files.move(part, checkpoint, StandardCopyOption.REPLACE_EXISTING)
        FileUtils.deleteDirectory(temporary.toFile)
        val path = new HadoopPath(checkpoint.toUri)
        val conf = spark.sparkContext.hadoopConfiguration
        val footer = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))
        try {
          for (bound <- Seq("minValues", "maxValues")) {
            val physical = footer.getFooter.getFileMetaData.getSchema
              .getType("add", "stats_parsed", bound, "v").asPrimitiveType()
            require(physical.getLogicalTypeAnnotation == null)
            require(physical.getPrimitiveTypeName == (if (integerType == "int")
              PrimitiveType.PrimitiveTypeName.INT32 else PrimitiveType.PrimitiveTypeName.INT64))
            println(s"Verified off-type checkpoint $kind/$integerType $bound: " +
              s"$physical value=$integerValue jsonFallback=${!parsedOnly}")
          }
        } finally footer.close()
        val reader = ParquetReader.builder[Group](new GroupReadSupport(), path)
          .withConf(conf).build()
        var count = 0
        try {
          var record = reader.read()
          while (record != null) {
            if (record.getFieldRepetitionCount("add") > 0) {
              count += 1
              val add = record.getGroup("add", 0)
              if (parsedOnly) require(add.getFieldRepetitionCount("stats") == 0) else {
                require(add.getFieldRepetitionCount("stats") == 1)
                val json = JsonUtil.mapper.readTree(add.getString("stats", 0))
                for (bound <- Seq("minValues", "maxValues")) {
                  val raw = json.at(s"/$bound/v").asText()
                  kind match {
                    case "timestamp" => require(Instant.parse(raw) == Instant.ofEpochSecond(1))
                    case "timestamp_ntz" => require(java.time.LocalDateTime.parse(raw) ==
                      java.time.LocalDateTime.of(1970, 1, 1, 0, 0, 1))
                    case "date" => require(java.time.LocalDate.parse(raw) ==
                      java.time.LocalDate.of(1970, 1, 2))
                  }
                }
              }
              val fields = add.getGroup("stats_parsed", 0)
              require(fields.getLong("numRecords", 0) == 3L)
              for (bound <- Seq("minValues", "maxValues")) {
                val values = fields.getGroup(bound, 0)
                val raw = if (integerType == "int") values.getInteger("v", 0).toLong
                  else values.getLong("v", 0)
                require(raw == integerValue)
              }
            }
            record = reader.read()
          }
        } finally reader.close()
        require(count == 1)
        readinessPruneHistory(dir, 1L)
        if (parsedOnly) {
          val adds = CommitLog.read(dir, 1L).collect { case add: AddFile => add }
          require(adds.size == 1 && adds.head.stats.isEmpty)
          val log = Files.list(dir.resolve("_delta_log"))
          val names = try log.iterator().asScala.map(_.getFileName.toString).toSet
            finally log.close()
          require(!names.exists(_.endsWith(".crc")))
          require(names.filter(_.endsWith(".json")) == Set("00000000000000000001.json"))
          println("Verified parsed-only DATE stats: INT32 day=1 (1970-01-02); JSON stats " +
            "absent in checkpoint and v1 commit; CRC removed; v0 pruned")
        }
      }
      readinessRead(table, "all", expectedSchema, rows)
      readinessRead(table, "all_v1", expectedSchema, rows, version = 1L)
      readinessRead(table, "hit", expectedSchema, rows, predicate = s"v = $literal")
      readinessRead(table, "miss", expectedSchema, Seq.empty,
        predicate = s"v < $literal OR v > $literal")
      readinessSnapshot(table, expectedSchema)
      readinessSnapshot(table, expectedSchema, version = 1L)
    }
  }

  private def readinessRemoveChecksums(dir: Path): Unit = {
    val files = Files.list(dir.resolve("_delta_log"))
    try files.iterator().asScala.filter(_.toString.endsWith(".crc"))
      .foreach(Files.delete(_)) finally files.close()
  }

  // JSON numeric stats are separate from physical checkpoint types in the checkpoint fixture.
  // Literal rows remain authoritative even when a reference parser discards off-type stats.
  for ((kind, sqlType, valueType, literal, value) <- Seq(
      ("timestamp", "TIMESTAMP", TimestampType, "TIMESTAMP '1970-01-01T00:00:01Z'",
        Timestamp.from(Instant.ofEpochSecond(1))),
      ("timestamp_ntz", "TIMESTAMP_NTZ", org.apache.spark.sql.types.TimestampNTZType,
        "TIMESTAMP_NTZ '1970-01-01 00:00:01'",
        java.time.LocalDateTime.of(1970, 1, 1, 0, 0, 1)),
      ("date", "DATE", org.apache.spark.sql.types.DateType,
        "DATE '1970-01-02'", java.sql.Date.valueOf("1970-01-02")),
      ("string", "STRING", org.apache.spark.sql.types.StringType, "'1'", "1"))) {
    test(s"action_integer_as_$kind") {
      spark.conf.set("spark.sql.session.timeZone", "UTC")
      val expectedSchema = new StructType().add("id", IntegerType).add("v", valueType)
      val rows = Seq(Row(1, value), Row(2, value), Row(2, value))
      sql(s"CREATE TABLE tbl (id INT, v $sqlType) USING delta")
      sql(s"INSERT INTO tbl SELECT /*+ COALESCE(1) */ * FROM VALUES " +
        s"(1, $literal), (2, $literal), (2, $literal) AS rows(id, v)")
      val table = registerTable("tbl")
      modifyCommitActions(table, 1L) { actions =>
        actions.map {
          case add: AddFile =>
            val stats = JsonUtil.mapper.readTree(add.stats.get)
            for (bound <- Seq("minValues", "maxValues")) {
              stats.get(bound).asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
                .put("v", 1)
            }
            add.copy(stats = Some(JsonUtil.mapper.writeValueAsString(stats)))
          case other => other
        }
      }
      mutateTable(table) { dir =>
        readinessRemoveChecksums(dir)
        val adds = CommitLog.read(dir, 1L).collect { case add: AddFile => add }
        require(adds.size == 1)
        val stats = JsonUtil.mapper.readTree(adds.head.stats.get)
        require(stats.get("numRecords").asLong() == 3L)
        for (bound <- Seq("minValues", "maxValues")) {
          require(stats.at(s"/$bound/v").isIntegralNumber)
          require(stats.at(s"/$bound/v").intValue() == 1)
        }
        println(s"Verified numeric JSON stats: $kind min=1 max=1 records=3")
      }
      // Spark accepts numeric STRING stats. The required-rejection probe is preserved
      // separately; this useful correctness fixture does not claim that rejection.
      readinessRead(table, "all", expectedSchema, rows)
      readinessRead(table, "all_v1", expectedSchema, rows, version = 1L)
      readinessRead(table, "hit", expectedSchema, rows, predicate = s"v = $literal")
      readinessRead(table, "miss", expectedSchema, Seq.empty,
        predicate = s"v < $literal OR v > $literal")
      readinessSnapshot(table, expectedSchema)
      readinessSnapshot(table, expectedSchema, version = 1L)
    }
  }

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
      spark.conf.set("spark.sql.session.timeZone", "UTC")
      // ANSI predicate casts reject empty numeric input. Leave legacy casts active for capture.
      // The existing spec format cannot transport this session setting to an external consumer.
      spark.conf.set("spark.sql.ansi.enabled", "false")
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
        readinessRemoveChecksums(dir)
        val adds = CommitLog.read(dir, 1L).collect { case add: AddFile => add }
        require(adds.size == 3)
        require(adds.count(_.partitionValues("p") == "") == 1)
        require(adds.count(_.partitionValues("p") == null) == 1)
        println(s"Verified serialized empty partition: $kind empty=1 null=1 control=1")
      }
      readinessRead(table, "all", expectedSchema, rows)
      readinessRead(table, "all_v1", expectedSchema, rows, version = 1L)
      readinessRead(table, "is_null", expectedSchema, rows.filter(_.isNullAt(1)),
        predicate = "p IS NULL")
      readinessRead(table, "is_not_null", expectedSchema, rows.filterNot(_.isNullAt(1)),
        predicate = "p IS NOT NULL")
      readinessRead(table, "eq_control", expectedSchema, rows.filter(_.getInt(0) == 2),
        predicate = s"p = $secondValue")
      if (kind == "string" || kind == "binary") {
        val emptyLiteral = if (kind == "string") "''" else "unhex('')"
        readinessRead(table, "eq_empty", expectedSchema, rows.filter(_.getInt(0) == 1),
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
      readinessSnapshot(table, expectedSchema, Seq("p"), version = 1L)
    }
  }

  // Inputs are literal AddFile strings, not SQL timestamp literal spellings.
  // The pinned Spark parser permits zone suffixes for NTZ casts and ignores their offsets;
  // expected NTZ wall clocks below are separate from the corresponding absolute instants.
  private def readinessPartitionFormats: Vector[(String, String, String, String)] = {
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
      spark.conf.set("spark.sql.session.timeZone", "UTC")
      spark.conf.set("spark.sql.ansi.enabled", "false")
      val formats = readinessPartitionFormats
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
        readinessRemoveChecksums(dir)
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
      readinessRead(table, "all", expectedSchema, rows)
      readinessRead(table, "all_v1", expectedSchema, rows, version = 1L)
      keys.values.toSet.toSeq.sorted.zipWithIndex.foreach { case (key, index) =>
        val ids = keys.collect { case (id, value) if value == key => id }.toSet
        readinessRead(table, s"eq_$index", expectedSchema,
          rows.filter(r => ids.contains(r.getInt(0))), predicate = s"p = $sqlType '$key'")
      }
      def selected(lower: String, upper: String): Seq[Row] = rows.filter { row =>
        val key = keys(row.getInt(0))
        key >= lower && key < upper
      }
      readinessRead(table, "fraction_range", expectedSchema,
        selected("2024-02-29T12:34:56.100000", "2024-02-29T12:34:56.123456"),
        predicate = s"p >= $sqlType '2024-02-29 12:34:56.100000' AND " +
          s"p < $sqlType '2024-02-29 12:34:56.123456'")
      readinessRead(table, "before_noon", expectedSchema,
        selected("2024-02-29T00:00:00", "2024-02-29T12:00:00"),
        predicate = s"p < $sqlType '2024-02-29 12:00:00'")
      readinessRead(table, "miss", expectedSchema, Seq.empty,
        predicate = s"p < $sqlType '2024-02-29 00:00:00'")
      readinessSnapshot(table, expectedSchema, Seq("p"))
      readinessSnapshot(table, expectedSchema, Seq("p"), version = 1L)
    }
  }

  // Canonical zero-padded DATE strings use proleptic Gregorian epoch-day oracles.
  test("date_partition_canonical_calendar_bounds") {
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    // Expected Parquet must retain proleptic Gregorian days even before the 1582 cutover.
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
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
      readinessRemoveChecksums(dir)
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
    readinessRead(table, allName, expectedSchema, rows)
    readinessRead(table, "all_v1", expectedSchema, rows, version = 1L)
    dates.foreach { case (id, raw, _) =>
      readinessRead(table, s"eq_$id", expectedSchema, rows.filter(_.getInt(0) == id),
        predicate = s"p = DATE '$raw'")
    }
    readinessRead(table, "is_null", expectedSchema, rows.filter(_.isNullAt(1)),
      predicate = "p IS NULL")
    readinessRead(table, "pre_epoch", expectedSchema, rows.filter(r => r.getInt(0) <= 4),
      predicate = "p < DATE '1970-01-01'")
    readinessRead(table, "calendar_transition", expectedSchema,
      rows.filter(r => Set(2, 3).contains(r.getInt(0))),
      predicate = "p >= DATE '1582-10-04' AND p <= DATE '1582-10-15'")
    readinessRead(table, "modern_range", expectedSchema,
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
    readinessSnapshot(table, expectedSchema, Seq("p"), version = 1L)
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

  // INT32 boundary magnitudes are BIGINT inputs to timestamp_seconds/millis/micros. The unit
  // names the input magnitude only; rewritten checkpoint cells are always TIMESTAMP_MICROS.
  private val intMaxInputs = Seq(-2147483648L, -2147483647L, -2147483646L,
    2147483646L, 2147483647L, 2147483648L)
  // Literal UTC instants per input unit, declared apart from both SQL and java.time.
  private val intMaxInstants = Seq(
    ("seconds", 1000000L, Seq("1901-12-13T20:45:52Z", "1901-12-13T20:45:53Z",
      "1901-12-13T20:45:54Z", "2038-01-19T03:14:06Z", "2038-01-19T03:14:07Z",
      "2038-01-19T03:14:08Z")),
    ("millis", 1000L, Seq("1969-12-07T03:28:36.352Z", "1969-12-07T03:28:36.353Z",
      "1969-12-07T03:28:36.354Z", "1970-01-25T20:31:23.646Z", "1970-01-25T20:31:23.647Z",
      "1970-01-25T20:31:23.648Z")),
    ("micros", 1L, Seq("1969-12-31T23:24:12.516352Z", "1969-12-31T23:24:12.516353Z",
      "1969-12-31T23:24:12.516354Z", "1970-01-01T00:35:47.483646Z",
      "1970-01-01T00:35:47.483647Z", "1970-01-01T00:35:47.483648Z")))
  // Data value index -> partition value index; no index maps to itself. Ids 2 and 5 hold the
  // -/+2147483647 boundaries twice, and id 7 is NULL in every temporal column twice.
  private val intMaxPartition = Map(1 -> 4, 2 -> 6, 3 -> 1, 4 -> 5, 5 -> 2, 6 -> 3)
  private val intMaxIds = Seq(1, 2, 2, 3, 4, 5, 5, 6, 7, 7)
  private val intMaxSchema = new StructType().add("id", IntegerType).add("ts", TimestampType)
    .add("ntz", TimestampNTZType).add("p_ts", TimestampType).add("p_ntz", TimestampNTZType)
  private val sixDigits =
    java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")

  for ((unit, scale, literals) <- intMaxInstants; parsed <- Seq(false, true)) {
    test(s"int_max_${unit}_${if (parsed) "parsed" else "json"}") {
      spark.conf.set("spark.sql.session.timeZone", "UTC")
      val instants = literals.map(Instant.parse)
      for ((input, instant) <- intMaxInputs.zip(instants)) {
        val converted = unit match {
          case "seconds" => Instant.ofEpochSecond(input)
          case "millis" => Instant.ofEpochMilli(input)
          case "micros" => Instant.EPOCH.plus(input, java.time.temporal.ChronoUnit.MICROS)
        }
        require(converted == instant && mismatchMicros(instant) == input * scale)
      }
      def wall(index: Int) =
        java.time.LocalDateTime.ofInstant(instants(index - 1), java.time.ZoneOffset.UTC)
      val rows = intMaxIds.map {
        case 7 => Row(7, null, null, null, null)
        case id =>
          val p = intMaxPartition(id)
          Row(id, Timestamp.from(instants(id - 1)), wall(id), Timestamp.from(instants(p - 1)),
            wall(p))
      }
      def partitionStrings(id: Int): Map[String, String] = if (id == 7) {
        Map("p_ts" -> null, "p_ntz" -> null)
      } else {
        val p = wall(intMaxPartition(id)).format(sixDigits)
        Map("p_ts" -> (p + "Z"),
          "p_ntz" -> p.replace('T', ' ').replaceAll("0+$", "").stripSuffix("."))
      }
      def fileId(add: AddFile): Int =
        JsonUtil.mapper.readTree(add.stats.get).at("/minValues/id").intValue()

      sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP, ntz TIMESTAMP_NTZ, p_ts TIMESTAMP,
        p_ntz TIMESTAMP_NTZ) USING delta PARTITIONED BY (p_ts, p_ntz) TBLPROPERTIES (
        'delta.checkpointPolicy' = 'classic', 'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.checkpoint.writeStatsAsJson' = 'false')""")
      val inputs = intMaxIds.map {
        case 7 => "(7, CAST(NULL AS BIGINT), CAST(NULL AS BIGINT))"
        case id => s"($id, ${intMaxInputs(id - 1)}L, ${intMaxInputs(intMaxPartition(id) - 1)}L)"
      }
      val f = s"timestamp_$unit"
      sql(s"INSERT INTO tbl SELECT /*+ COALESCE(1) */ id, $f(v), CAST($f(v) AS TIMESTAMP_NTZ), " +
        s"$f(p), CAST($f(p) AS TIMESTAMP_NTZ) FROM VALUES ${inputs.mkString(", ")} " +
        "AS rows(id, v, p)")
      val table = registerTable("tbl")
      val produced = CommitLog.read(table.sourcePath, 1L).collect { case a: AddFile => a }
      require(produced.map(a => fileId(a) -> a.partitionValues).toMap ==
        (1 to 7).map(id => id -> partitionStrings(id)).toMap && produced.size == 7)

      if (parsed) forceCheckpoint("tbl") else {
        // The producer writes millisecond JSON bounds. Keep all six fraction digits instead.
        modifyCommitActions(table, 1L) { actions =>
          actions.map {
            case add: AddFile if fileId(add) != 7 =>
              val id = fileId(add)
              println(s"Producer JSON stats before rewrite: $unit id=$id ${add.stats.get}")
              val stats = JsonUtil.mapper.readTree(add.stats.get)
              for (bound <- Seq("minValues", "maxValues")) {
                stats.get(bound).asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
                  .put("ts", wall(id).format(sixDigits) + "Z")
                  .put("ntz", wall(id).format(sixDigits))
              }
              add.copy(stats = Some(JsonUtil.mapper.writeValueAsString(stats)))
            case other => other
          }
        }
      }
      mutateTable(table) { dir =>
        if (parsed) {
          val checkpoint = dir.resolve("_delta_log/00000000000000000001.checkpoint.parquet")
          rewriteIntMaxCheckpoint(checkpoint, instants)
          checkIntMaxCheckpoint(checkpoint, instants, partitionStrings)
          readinessPruneHistory(dir, 1L)
        } else {
          readinessRemoveChecksums(dir)
          val adds = CommitLog.read(dir, 1L).collect { case a: AddFile => a }
          require(adds.size == 7)
          for (add <- adds) {
            val id = fileId(add)
            val stats = JsonUtil.mapper.readTree(add.stats.get)
            require(add.partitionValues == partitionStrings(id))
            require(stats.get("numRecords").asLong() == (if (Set(2, 5, 7)(id)) 2L else 1L))
            for (bound <- Seq("minValues", "maxValues")) {
              if (id == 7) require(!stats.get(bound).has("ts") && !stats.get(bound).has("ntz"))
              else {
                require(stats.at(s"/$bound/ts").asText() == wall(id).format(sixDigits) + "Z")
                require(stats.at(s"/$bound/ntz").asText() == wall(id).format(sixDigits))
              }
            }
            if (id == 7) require(stats.at("/nullCount/ts").asLong() == 2L)
          }
          println(s"Verified six-digit JSON stats and partitions: $unit files=7")
        }
      }

      readinessRead(table, "all", intMaxSchema, rows)
      readinessRead(table, "all_v1", intMaxSchema, rows, version = 1L)
      for ((field, kind) <- Seq("ts" -> "TIMESTAMP", "ntz" -> "TIMESTAMP_NTZ",
          "p_ts" -> "TIMESTAMP", "p_ntz" -> "TIMESTAMP_NTZ")) {
        def v(index: Int): String = if (kind == "TIMESTAMP") {
          s"TIMESTAMP '${instants(index - 1)}'"
        } else s"TIMESTAMP_NTZ '${wall(index).format(sixDigits).replace('T', ' ')}'"
        def index(row: Row): Option[Int] = row.getInt(0) match {
          case 7 => None
          case id => Some(if (field.startsWith("p_")) intMaxPartition(id) else id)
        }
        for ((label, predicate, selected) <- Seq(
            ("below_negative", s"$field < ${v(2)}", Set(1)),
            ("eq_negative", s"$field = ${v(2)}", Set(2)),
            ("interior", s"$field > ${v(2)} AND $field < ${v(5)}", Set(3, 4)),
            ("eq_positive", s"$field = ${v(5)}", Set(5)),
            ("above_positive", s"$field > ${v(5)}", Set(6)),
            ("outside", s"$field < ${v(1)} OR $field > ${v(6)}", Set.empty[Int]))) {
          readinessRead(table, s"${field}_$label", intMaxSchema,
            rows.filter(r => index(r).exists(selected)), predicate)
        }
        readinessRead(table, s"${field}_is_null", intMaxSchema,
          rows.filter(_.getInt(0) == 7), s"$field IS NULL")
      }
      readinessSnapshot(table, intMaxSchema, Seq("p_ts", "p_ntz"))
      readinessSnapshot(table, intMaxSchema, Seq("p_ts", "p_ntz"), version = 1L)
    }
  }

  // DATE32 epoch-day bounds. 2147483648 is outside DATE32 and is not an input. Rows use
  // java.time.LocalDate: java.sql.Date misrenders these years. Raw INT32 days are the oracle.
  test("date_epoch_days_int32_bounds") {
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    // Spark's data-filter translation rebases java.sql.Date literals to Julian days, which
    // overflows near -2147483648. LocalDate literals avoid that reference-side conversion.
    // Capture runs after this body, so afterEach restores the setting, including on failure.
    java8Previous = Some(spark.conf.get(java8Key))
    spark.conf.set(java8Key, "true")
    val days = Seq(-2147483648, -2147483647, -2147483646, 2147483646, 2147483647)
    val dates = Seq("-5877641-06-23", "-5877641-06-24", "-5877641-06-25", "+5881580-07-10",
      "+5881580-07-11").map(LocalDate.parse)
    require(dates.map(_.toEpochDay) == days.map(_.toLong))
    val permutation = Map(1 -> 3, 2 -> 5, 3 -> 1, 4 -> 2, 5 -> 4)
    val ids = Seq(1, 2, 2, 3, 4, 5, 5, 6, 6)
    val expectedSchema = new StructType().add("id", IntegerType)
      .add("d", org.apache.spark.sql.types.DateType).add("p", org.apache.spark.sql.types.DateType)
    val rows = ids.map {
      case 6 => Row(6, null, null)
      case id => Row(id, dates(id - 1), dates(permutation(id) - 1))
    }
    val units = ids.map {
      case 6 => (6, None, None)
      case id => (id, Some(days(id - 1)), Some(days(permutation(id) - 1)))
    }
    sql("CREATE TABLE tbl (id INT, d DATE, p DATE) USING delta PARTITIONED BY (p)")
    val inputs = ids.map {
      case 6 => "(6, CAST(NULL AS INT), CAST(NULL AS INT))"
      case id => s"($id, ${days(id - 1)}, ${days(permutation(id) - 1)})"
    }
    sql("INSERT INTO tbl SELECT /*+ COALESCE(1) */ id, date_from_unix_date(d), " +
      s"date_from_unix_date(p) FROM VALUES ${inputs.mkString(", ")} AS rows(id, d, p)")
    val table = registerTable("tbl")
    mutateTable(table) { dir =>
      readinessRemoveChecksums(dir)
      val adds = CommitLog.read(dir, 1L).collect { case add: AddFile => add }
      require(adds.size == 6)
      adds.foreach { add =>
        val stats = JsonUtil.mapper.readTree(add.stats.get)
        val id = stats.at("/minValues/id").intValue()
        require(stats.get("numRecords").asLong() == (if (Set(2, 5, 6)(id)) 2L else 1L))
        if (id == 6) {
          require(add.partitionValues == Map("p" -> null))
          require(stats.at("/nullCount/d").asLong() == 2L && !stats.at("/minValues").has("d"))
        } else {
          require(add.partitionValues == Map("p" -> dates(permutation(id) - 1).toString))
          for (bound <- Seq("minValues", "maxValues")) {
            require(stats.at(s"/$bound/d").asText() == dates(id - 1).toString)
          }
        }
        println(s"Verified DATE32 bound file: id=$id stats=${add.stats.get} " +
          s"partition=${add.partitionValues}")
      }
    }
    def literal(index: Int): String = s"DATE '${dates(index - 1)}'"
    readinessRead(table, "all", expectedSchema, rows)
    readinessRead(table, "all_v1", expectedSchema, rows, version = 1L)
    for (field <- Seq("d", "p")) {
      def index(row: Row): Option[Int] = row.getInt(0) match {
        case 6 => None
        case id => Some(if (field == "p") permutation(id) else id)
      }
      for ((label, predicate, selected) <- Seq(
          ("below_negative", s"$field < ${literal(2)}", Set(1)),
          ("eq_negative", s"$field = ${literal(2)}", Set(2)),
          ("interior", s"$field > ${literal(2)} AND $field < ${literal(5)}", Set(3, 4)),
          ("eq_positive", s"$field = ${literal(5)}", Set(5)))) {
        readinessRead(table, s"${field}_$label", expectedSchema,
          rows.filter(r => index(r).exists(selected)), predicate)
      }
      readinessRead(table, s"${field}_is_null", expectedSchema,
        rows.filter(_.getInt(0) == 6), s"$field IS NULL")
    }
    readinessSnapshot(table, expectedSchema, Seq("p"))
    readinessSnapshot(table, expectedSchema, Seq("p"), version = 1L)
    // Snapshots capture before reads at this pin. Inspect Parquet in the last read callback.
    val workload = WorkloadContext.current.workloadName
    readSpec(table, predicate = s"d < ${literal(1)} OR d > ${literal(5)}",
      name = Some("outside")).assert { spec =>
      spec.expectation match {
        case Succeeded(result) => require(result.rowCount == 0L)
        case other => fail(s"Outside DATE32 bounds must succeed with no rows: $other")
      }
      val conf = spark.sparkContext.hadoopConfiguration
      val captured = outputDir.resolve(workload).resolve("expected")
        .resolve(s"${workload}_all").resolve("expected_data")
      val literalOracle = outputDir.resolveSibling("oracle-parquet").resolve(workload)
      for ((label, directory) <- Seq("captured" -> captured, "literal" -> literalOracle)) {
        val files = Files.list(directory)
        val parquet = try files.iterator().asScala.filter(_.toString.endsWith(".parquet"))
          .toVector finally files.close()
        require(parquet.nonEmpty)
        val found = parquet.flatMap { file =>
          val path = new HadoopPath(file.toUri)
          val footer = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))
          try for (field <- Seq("d", "p")) {
            val physical = footer.getFooter.getFileMetaData.getSchema.getFields.asScala
              .find(_.getName == field).get.asPrimitiveType()
            require(physical.getPrimitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32)
            require(physical.getLogicalTypeAnnotation == LogicalTypeAnnotation.dateType())
          } finally footer.close()
          val reader = ParquetReader.builder[Group](new GroupReadSupport(), path)
            .withConf(conf).build()
          var out = Vector.empty[(Int, Option[Int], Option[Int])]
          try {
            var record = reader.read()
            while (record != null) {
              def day(field: String) = if (record.getFieldRepetitionCount(field) == 0) None
                else Some(record.getInteger(field, 0))
              out :+= ((record.getInteger("id", 0), day("d"), day("p")))
              record = reader.read()
            }
          } finally reader.close()
          out
        }
        require(found.sorted == units.sorted)
        println(s"Verified raw DATE32 epoch days: $workload $label ${found.sorted}")
      }
      require(spark.conf.get(java8Key) == "true")
    }
  }

  private val java8Key = "spark.sql.datetime.java8API.enabled"
  private var java8Previous: Option[String] = None

  override def afterEach(): Unit = {
    try super.afterEach() finally {
      java8Previous.foreach(previous => if (_spark != null) _spark.conf.set(java8Key, previous))
      java8Previous = None
    }
  }

  // Replace producer-derived parsed bounds by the literal instants and drop the JSON fallback.
  private def rewriteIntMaxCheckpoint(checkpoint: Path, instants: Seq[Instant]): Unit = {
    val rewrite = checkpoint.resolveSibling("int-max-checkpoint-rewrite")
    val confKey = "spark.sql.parquet.outputTimestampType"
    val previous = spark.conf.get(confKey)
    try {
      spark.conf.set(confKey, "TIMESTAMP_MICROS")
      val df = spark.read.parquet(checkpoint.toString)
      var stats = col("add.stats_parsed")
      for (bound <- Seq("minValues", "maxValues")) {
        var ts = col(s"add.stats_parsed.$bound.ts")
        var ntz = col(s"add.stats_parsed.$bound.ntz")
        for ((instant, index) <- instants.zipWithIndex) {
          val matches = col("add.stats_parsed.minValues.id") === lit(index + 1)
          ts = when(matches, lit(Timestamp.from(instant))).otherwise(ts)
          ntz = when(matches,
            lit(java.time.LocalDateTime.ofInstant(instant, java.time.ZoneOffset.UTC)))
            .otherwise(ntz)
        }
        stats = stats.withField(bound, col(s"add.stats_parsed.$bound")
          .withField("ts", ts).withField("ntz", ntz))
      }
      var add = col("add").withField("stats_parsed", stats)
      if (df.schema("add").dataType.asInstanceOf[StructType].fieldNames.contains("stats")) {
        add = add.withField("stats", lit(null).cast("string"))
      }
      df.withColumn("add", add).coalesce(1).write.parquet(rewrite.toString)
      val files = Files.list(rewrite)
      val part = try files.iterator().asScala.filter(_.toString.endsWith(".parquet"))
        .toSeq match {
          case Seq(single) => single
          case other => fail(s"Expected one rewritten INT_MAX checkpoint: $other")
        } finally files.close()
      Files.move(part, checkpoint, StandardCopyOption.REPLACE_EXISTING)
    } finally {
      spark.conf.set(confKey, previous)
      if (Files.exists(rewrite)) FileUtils.deleteDirectory(rewrite.toFile)
    }
  }

  private def checkIntMaxCheckpoint(
      checkpoint: Path, instants: Seq[Instant], partitions: Int => Map[String, String]): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration
    val path = new HadoopPath(checkpoint.toUri)
    val footer = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))
    try {
      val physical = footer.getFooter.getFileMetaData.getSchema
      for ((prefix, field) <- Seq("stats_parsed.minValues" -> "ts",
          "stats_parsed.minValues" -> "ntz", "stats_parsed.maxValues" -> "ts",
          "stats_parsed.maxValues" -> "ntz", "partitionValues_parsed" -> "p_ts",
          "partitionValues_parsed" -> "p_ntz")) {
        val cell = physical.getType((Seq("add") ++ prefix.split("\\.") :+ field): _*)
          .asPrimitiveType()
        require(cell.getPrimitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64)
        val annotation = cell.getLogicalTypeAnnotation
          .asInstanceOf[LogicalTypeAnnotation.TimestampLogicalTypeAnnotation]
        require(annotation.getUnit == LogicalTypeAnnotation.TimeUnit.MICROS)
        require(annotation.isAdjustedToUTC == !field.endsWith("ntz"))
        println(s"Verified INT_MAX checkpoint $prefix.$field: $cell")
      }
    } finally footer.close()
    val reader = ParquetReader.builder[Group](new GroupReadSupport(), path).withConf(conf).build()
    var ids = Vector.empty[Int]
    try {
      var record = reader.read()
      while (record != null) {
        if (record.getFieldRepetitionCount("add") > 0) {
          val add = record.getGroup("add", 0)
          if (add.getType.containsField("stats")) require(add.getFieldRepetitionCount("stats") == 0)
          val stats = add.getGroup("stats_parsed", 0)
          val id = stats.getGroup("minValues", 0).getInteger("id", 0)
          require(stats.getLong("numRecords", 0) == (if (Set(2, 5, 7)(id)) 2L else 1L))
          val parsedPartition = add.getGroup("partitionValues_parsed", 0)
          for (bound <- Seq("minValues", "maxValues")) {
            val values = stats.getGroup(bound, 0)
            require(values.getInteger("id", 0) == id)
            for (field <- Seq("ts", "ntz")) {
              if (id == 7) require(values.getFieldRepetitionCount(field) == 0)
              else require(values.getLong(field, 0) == mismatchMicros(instants(id - 1)))
            }
          }
          for (field <- Seq("p_ts", "p_ntz")) {
            if (id == 7) require(parsedPartition.getFieldRepetitionCount(field) == 0)
            else require(parsedPartition.getLong(field, 0) ==
              mismatchMicros(instants(intMaxPartition(id) - 1)))
          }
          if (id == 7) require(stats.getGroup("nullCount", 0).getLong("ts", 0) == 2L)
          val raw = add.getGroup("partitionValues", 0)
          val partition = (0 until raw.getFieldRepetitionCount("key_value")).map { i =>
            val entry = raw.getGroup("key_value", i)
            val value = if (entry.getFieldRepetitionCount("value") == 0) null
              else entry.getString("value", 0)
            entry.getString("key", 0) -> value
          }.toMap
          require(partition == partitions(id))
          ids :+= id
          println(s"Verified INT_MAX raw checkpoint id=$id partition=$partition")
        }
        record = reader.read()
      }
    } finally reader.close()
    require(ids.sorted == (1 to 7).toVector)
  }
}
