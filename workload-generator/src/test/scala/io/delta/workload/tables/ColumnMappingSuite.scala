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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}

import io.delta.workload.{WorkloadContext, WorkloadTestSuite}
import io.delta.workload.engine.RowComparison
import io.delta.workload.json.JsonUtil
import io.delta.workload.log.{AddFile, CommitLog, Metadata => LogMetadata, Protocol, RemoveFile}
import io.delta.workload.model.Succeeded

class ColumnMappingSuite extends WorkloadTestSuite("column_mapping") {

  // Existing 6 workloads

  test("mode_name") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1,'alice',100),(2,'bob',200),(3,'charlie',300)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("name", "value")))
    readSpec(t, predicate = "value > 150")
    snapshotSpec(t)
  }

  test("rename_column") {
    sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob')")
    sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    sql("INSERT INTO tbl VALUES (3,'charlie')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 2)
    readSpec(t, columns = Some(Seq("id", "new_name")))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("drop_column") {
    sql("""CREATE TABLE tbl (id INT, name STRING, to_drop STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'a','drop1'),(2,'b','drop2')")
    sql("ALTER TABLE tbl DROP COLUMN to_drop")
    sql("INSERT INTO tbl VALUES (3,'c')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 2)
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("drop_readd") {
    sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'hello')")
    sql("ALTER TABLE tbl DROP COLUMN x")
    sql("ALTER TABLE tbl ADD COLUMNS (x INT)")
    sql("INSERT INTO tbl VALUES (2, 42)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "x IS NULL")
    readSpec(t, predicate = "x IS NOT NULL")
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("nested_columns") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "info.age > 27")
    snapshotSpec(t)
  }

  test("mode_upgrade") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'before'),(2,'before')")
    sql("""ALTER TABLE tbl SET TBLPROPERTIES (
      'delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (3,'after')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("mode_id") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'id')""")
    sql("INSERT INTO tbl VALUES (1,'alpha'),(2,'beta'),(3,'gamma')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("name")))
    readSpec(t, predicate = "id > 1")
    snapshotSpec(t)
  }

  test("upgrade") {
    // Similar to cm_mode_upgrade but exercises version-by-version reading
    sql("CREATE TABLE tbl (a INT, b STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'x'),(2,'y')")
    sql("""ALTER TABLE tbl SET TBLPROPERTIES (
      'delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (3,'z')")
    sql("ALTER TABLE tbl RENAME COLUMN b TO c")
    sql("INSERT INTO tbl VALUES (4,'w')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, columns = Some(Seq("a", "c")))
    for (v <- 0L to 5L) snapshotSpec(t, version = v)
  }

  test("mode_upgrade_partitioned") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'a')")
    sql("""ALTER TABLE tbl SET TBLPROPERTIES (
      'delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (4,'c')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "part = 'a'")
    readSpec(t, version = 0)
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("array_of_structs") {
    sql("""CREATE TABLE tbl (id INT, items ARRAY<STRUCT<name: STRING, qty: INT>>) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, array(named_struct('name','apple','qty',3)))")
    sql("INSERT INTO tbl VALUES (2, array(named_struct('name','banana','qty',5), named_struct('name','cherry','qty',2)))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("items")))
    snapshotSpec(t)
  }

  test("complex_types") {
    sql("""CREATE TABLE tbl (id INT, tags ARRAY<STRING>, props MAP<STRING, INT>) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, array('a','b'), map('x',1,'y',2))")
    sql("INSERT INTO tbl VALUES (2, array('c'), map('z',3))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("tags")))
    readSpec(t, columns = Some(Seq("props")))
    snapshotSpec(t)
  }

  test("map_type") {
    sql("""CREATE TABLE tbl (id INT, data MAP<STRING, STRING>) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, map('key1','val1','key2','val2'))")
    sql("INSERT INTO tbl VALUES (2, map('key3','val3'))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("data")))
    snapshotSpec(t)
  }

  test("deeply_nested") {
    sql("""CREATE TABLE tbl (
      id INT,
      l1 STRUCT<l2: STRUCT<l3: STRUCT<value: STRING>>>
    ) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('l2', named_struct('l3', named_struct('value', 'deep'))))")
    sql("INSERT INTO tbl VALUES (2, named_struct('l2', named_struct('l3', named_struct('value', 'deeper'))))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("l1")))
    snapshotSpec(t)
  }

  test("drop_readd_same_name") {
    sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'text')")
    sql("ALTER TABLE tbl DROP COLUMN x")
    sql("ALTER TABLE tbl ADD COLUMNS (x DOUBLE)")
    sql("INSERT INTO tbl VALUES (2, 3.14)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "x IS NOT NULL")
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("predicate_after_rename") {
    sql("""CREATE TABLE tbl (id INT, a INT) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 100),(2, 200),(3, 300)")
    sql("ALTER TABLE tbl RENAME COLUMN a TO b")
    sql("INSERT INTO tbl VALUES (4, 400)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "b > 200")
    readSpec(t, predicate = "b = 100")
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("predicate_on_readded") {
    sql("""CREATE TABLE tbl (id INT, val INT) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 10),(2, 20)")
    sql("ALTER TABLE tbl DROP COLUMN val")
    sql("ALTER TABLE tbl ADD COLUMNS (val INT)")
    sql("INSERT INTO tbl VALUES (3, 30),(4, 40)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "val > 25")
    readSpec(t, predicate = "val IS NULL")
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("predicate_renamed_partition") {
    sql("""CREATE TABLE tbl (id INT, part STRING) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'x'),(2,'y'),(3,'x')")
    sql("ALTER TABLE tbl RENAME COLUMN part TO region")
    sql("INSERT INTO tbl VALUES (4,'z')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "region = 'x'")
    readSpec(t, predicate = "region = 'z'")
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("rename_partition_col") {
    sql("""CREATE TABLE tbl (id INT, category STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'a')")
    sql("ALTER TABLE tbl RENAME COLUMN category TO cat")
    sql("INSERT INTO tbl VALUES (4,'c')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "cat = 'a'")
    readSpec(t, columns = Some(Seq("id", "cat")))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("nested_struct_name") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<first: STRING, last: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('first','alice','last','smith','age',30))")
    sql("INSERT INTO tbl VALUES (2, named_struct('first','bob','last','jones','age',25))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("info")))
    readSpec(t, columns = Some(Seq("id")))
    snapshotSpec(t)
  }

  test("nested_struct_id") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, score: DOUBLE>) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'id')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','score',95.5))")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','score',87.3))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("info")))
    snapshotSpec(t)
  }

  test("nested_rename_3_levels") {
    sql("""CREATE TABLE tbl (
      id INT,
      outer_col STRUCT<mid: STRUCT<inner_val: STRING>>
    ) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('mid', named_struct('inner_val','hello')))")
    sql("ALTER TABLE tbl RENAME COLUMN outer_col.mid.inner_val TO renamed_val")
    sql("INSERT INTO tbl VALUES (2, named_struct('mid', named_struct('renamed_val','world')))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("filter_pushdown_physical_names") {
    // Create with name mode, rename column, verify filter uses physical name correctly
    sql("""CREATE TABLE tbl (a INT, b STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (100,'first'),(200,'second')")
    sql("ALTER TABLE tbl RENAME COLUMN a TO c")
    sql("INSERT INTO tbl VALUES (300,'third'),(1000,'fourth')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "c = 1000", name = Some("read_filter_c_eq_1000"))
    readSpec(t, predicate = "c = 100", name = Some("read_filter_c_eq_100"))
    readSpec(t, predicate = "c > 200", name = Some("read_filter_c_gt_200"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("physical_name_matches_logical") {
    // After rename, old physical name may match some other column's logical name
    sql("""CREATE TABLE tbl (id INT, alpha STRING, beta STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'a1', 'b1')")
    sql("ALTER TABLE tbl RENAME COLUMN alpha TO gamma")
    sql("INSERT INTO tbl VALUES (2, 'a2', 'b2')")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("id_mode_rename_projection") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'id',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'before1'),(2,'before2')")
    sql("ALTER TABLE tbl RENAME COLUMN value TO renamed_value")
    sql("INSERT INTO tbl VALUES (3,'after1'),(4,'after2')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("renamed_value")), name = Some("read_project_renamed_col"))
    readSpec(t, columns = Some(Seq("id")), name = Some("read_project_id_only"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("id_mode_schema_evolution") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'id',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1),(2)")
    sql("ALTER TABLE tbl ADD COLUMNS (new_col STRING)")
    sql("INSERT INTO tbl VALUES (3, 'hello')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("id_matching_swapped") {
    // Create table in id mode with nested struct, manipulate column IDs
    sql("""CREATE TABLE tbl (a STRING, b STRUCT<c: STRING, d: INT>) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'id')""")
    sql("INSERT INTO tbl VALUES ('hello', named_struct('c','world','d',42))")
    // Swap column mapping IDs by altering metadata — use ALTER TABLE to change schema
    sql("ALTER TABLE tbl RENAME COLUMN a TO e")
    sql("INSERT INTO tbl VALUES ('swapped', named_struct('c','test','d',99))")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_select_a_reads_e"))
    snapshotSpec(t)
  }

  test("id_matching_nonexistent") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'id',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'exists')")
    // Drop and add column — old physical data has non-matching ID
    sql("ALTER TABLE tbl DROP COLUMN name")
    sql("ALTER TABLE tbl ADD COLUMNS (name STRING)")
    sql("INSERT INTO tbl VALUES (2,'new')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "name IS NULL")
    snapshotSpec(t)
  }

  test("projection_complex_types") {
    sql("""CREATE TABLE tbl (
      id INT,
      arr ARRAY<INT>,
      mp MAP<STRING, INT>,
      st STRUCT<a: STRING, b: INT>
    ) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, array(10,20), map('x',1), named_struct('a','hello','b',42))")
    sql("INSERT INTO tbl VALUES (2, array(30), map('y',2,'z',3), named_struct('a','world','b',99))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("arr")), name = Some("read_project_array_only"))
    readSpec(t, columns = Some(Seq("mp")), name = Some("read_project_map_only"))
    readSpec(t, columns = Some(Seq("st")), name = Some("read_project_struct_only"))
    snapshotSpec(t)
  }

  test("select_after_drop") {
    sql("""CREATE TABLE tbl (id INT, keep STRING, drop_me INT, extra DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'a', 10, 1.1),(2, 'b', 20, 2.2)")
    sql("ALTER TABLE tbl DROP COLUMN drop_me")
    sql("INSERT INTO tbl VALUES (3, 'c', 3.3)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "keep")), name = Some("read_project_remaining_columns"))
    readSpec(t, columns = Some(Seq("extra")), name = Some("read_project_extra_only"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  // Parsed checkpoint statistics/partition values keyed by physical names, and enabling
  // name mapping through a full rewrite.
  // One primary scenario covers parsed statistics and parsed partition values together.
  // The existing checkpoints_partitioned fixture is the no-mapping parsed-key control.
  for (mode <- Seq("name", "id")) {
    test(s"parsed_physical_names_$mode") {
      val properties = Map(
        "delta.columnMapping.mode" -> mode,
        "delta.checkpointInterval" -> "1000",
        "delta.checkpoint.writeStatsAsStruct" -> "true",
        "delta.checkpoint.writeStatsAsJson" -> "false")
      val propertySql = properties.toSeq.sortBy(_._1)
        .map { case (key, value) => s"'$key' = '$value'" }.mkString(", ")
      sql(s"""CREATE TABLE tbl (
        id INT, old_value INT, payload STRUCT<old_leaf: INT>, old_part STRING)
        USING delta PARTITIONED BY (old_part) TBLPROPERTIES ($propertySql)""")
      sql("""INSERT INTO tbl VALUES
        (1, 10, named_struct('old_leaf', -10), 'east'),
        (1, 10, named_struct('old_leaf', -10), 'east'),
        (2, 20, named_struct('old_leaf', -20), 'west'),
        (3, NULL, named_struct('old_leaf', CAST(NULL AS INT)), NULL),
        (4, 40, NULL, 'east')""")
      sql("ALTER TABLE tbl RENAME COLUMN old_value TO value")
      sql("ALTER TABLE tbl RENAME COLUMN payload.old_leaf TO leaf")
      sql("ALTER TABLE tbl RENAME COLUMN old_part TO region")
      sql("""INSERT INTO tbl VALUES
        (5, 100, named_struct('leaf', -100), 'north'),
        (6, NULL, named_struct('leaf', CAST(NULL AS INT)), NULL),
        (7, 200, named_struct('leaf', -200), 'east')""")
      forceCheckpoint("tbl")
      // A real v6 tail is replayed after the v5 checkpoint; v5 JSON is only version lookup.
      sql("INSERT INTO tbl VALUES (8, 300, named_struct('leaf', -300), 'west')")
      val t = registerTable("tbl")
      val table = WorkloadContext.current.getTableSpec(t)
      val actions = (0L to 6L).map { version =>
        version -> CommitLog.readRaw(t.sourcePath, version).map(JsonUtil.mapper.readTree)
      }.toMap
      val beforeMetadata = actions(0L).find(_.has("metaData")).get.get("metaData")
      val metadata = actions(4L).find(_.has("metaData")).get.get("metaData")
      val before = DataType.fromJson(beforeMetadata.get("schemaString").asText)
        .asInstanceOf[StructType]
      val mapped = DataType.fromJson(metadata.get("schemaString").asText)
        .asInstanceOf[StructType]
      val physicalKey = "delta.columnMapping.physicalName"
      val idKey = "delta.columnMapping.id"
      val oldFields = before.fields.toSeq :+
        before("payload").dataType.asInstanceOf[StructType]("old_leaf")
      val fields = mapped.fields.toSeq :+
        mapped("payload").dataType.asInstanceOf[StructType]("leaf")
      assert(mapped.fieldNames.toSeq == Seq("id", "value", "payload", "region"))
      assert(fields.map(_.metadata.getLong(idKey)).toSet == Set(1L, 2L, 3L, 4L, 5L))
      assert(fields.map(_.metadata) == oldFields.map(_.metadata))
      assert(fields.forall(field => field.metadata.getString(physicalKey) != field.name))
      val physical = fields.map(_.metadata.getString(physicalKey))
      assert(physical.distinct.size == 5)
      val partitionName = physical(3)
      val statsNames = physical.take(3).toSet
      val originals = actions.values.flatten.filter(_.has("add"))
        .map(_.get("add")).map(add => add.get("path").asText -> add).toMap

      mutateTable(t) { tableDir =>
        val logDir = tableDir.resolve("_delta_log")
        for (version <- 0L to 6L; line <- CommitLog.readRaw(tableDir, version)) {
          println(s"Physical-name source action: $mode v$version $line")
        }
        val checkpointPath = logDir.resolve("00000000000000000005.checkpoint.parquet")
        assert(Files.exists(checkpointPath))
        val checkpoint = spark.read.parquet(checkpointPath.toString)
        val addSchema = checkpoint.schema("add").dataType.asInstanceOf[StructType]
        assert(!addSchema.fieldNames.contains("stats"))
        val partition = addSchema("partitionValues_parsed").dataType.asInstanceOf[StructType]
        assert(partition.fieldNames.toSeq == Seq(partitionName))
        assert(partition.fields.head.dataType == StringType)
        val stats = addSchema("stats_parsed").dataType.asInstanceOf[StructType]
        for (kind <- Seq("minValues", "maxValues", "nullCount")) {
          val leaves = stats(kind).dataType.asInstanceOf[StructType]
          assert(leaves.fieldNames.toSet == statsNames)
          assert(leaves(physical(2)).dataType.asInstanceOf[StructType]
            .fieldNames.toSeq == Seq(physical(4)))
        }
        val adds = checkpoint.filter("add IS NOT NULL").select("add").toJSON.collect()
        assert(adds.nonEmpty)
        for (line <- adds) {
          val add = JsonUtil.mapper.readTree(line).get("add")
          val original = originals(add.get("path").asText)
          assert(add.get("partitionValues") == original.get("partitionValues"))
          assert(add.get("partitionValues").fieldNames().asScala.toSet == Set(partitionName))
          val parsedPartition = add.get("partitionValues_parsed").get(partitionName)
          val rawPartition = original.get("partitionValues").get(partitionName)
          assert(Option(parsedPartition).filterNot(_.isNull).map(_.asText) ==
            Option(rawPartition).filterNot(_.isNull).map(_.asText))
          val rawStats = JsonUtil.mapper.readTree(original.get("stats").asText)
          val parsedStats = add.get("stats_parsed")
          assert(parsedStats.get("numRecords") == rawStats.get("numRecords"))
          for (kind <- Seq("minValues", "maxValues", "nullCount");
              path <- Seq(Seq(physical(0)), Seq(physical(1)), Seq(physical(2), physical(4)))) {
            val parsed = path.foldLeft(parsedStats.path(kind))((node, key) => node.path(key))
            val raw = path.foldLeft(rawStats.path(kind))((node, key) => node.path(key))
            assert(Option(parsed).filterNot(n => n.isNull || n.isMissingNode) ==
              Option(raw).filterNot(n => n.isNull || n.isMissingNode))
          }
        }
        // Earlier protocol/metadata/file history is absent: v5 checkpoint remains required.
        // Keep v5 for version lookup and v6 as the actual post-checkpoint replay tail.
        val stream = Files.list(logDir)
        val files = try stream.iterator().asScala.toSeq finally stream.close()
        files.filter { path =>
          val name = path.getFileName.toString
          name.endsWith(".crc") ||
            (name.matches("[0-9]{20}\\.json") && name < "00000000000000000005.json")
        }.foreach(Files.delete)
        println(s"Physical-name checkpoint/layout oracle passed: $mode")
      }

      val schema = new StructType().add("id", IntegerType).add("value", IntegerType)
        .add("payload", new StructType().add("leaf", IntegerType)).add("region", StringType)
      val rows = Seq(
        Row(1, 10, Row(-10), "east"), Row(1, 10, Row(-10), "east"),
        Row(2, 20, Row(-20), "west"), Row(3, null, Row(null), null),
        Row(4, 40, null, "east"), Row(5, 100, Row(-100), "north"),
        Row(6, null, Row(null), null), Row(7, 200, Row(-200), "east"),
        Row(8, 300, Row(-300), "west"))
      // Row selections are literal bag oracles, independent of Spark's predicate evaluation.
      val reads = Seq(
        ("read_all", null, rows.indices, None, None),
        ("read_checkpoint", null, 0 until 8, Some(5L), None),
        ("read_old_file", "value = 10", Seq(0, 1), None, None),
        ("read_new_file", "value = 100", Seq(5), None, None),
        ("read_tail", "value = 300", Seq(8), None, None),
        ("read_missing", "value = 999", Seq.empty[Int], None, None),
        ("read_null", "value IS NULL", Seq(3, 6), None, None),
        ("read_nested_old", "payload.leaf = -10", Seq(0, 1), None, None),
        ("read_nested_new", "payload.leaf = -100", Seq(5), None, None),
        ("read_nested_null", "payload.leaf IS NULL", Seq(3, 4, 6), None, None),
        ("read_partition_old", "region = 'east'", Seq(0, 1, 4, 7), None, None),
        ("read_partition_new", "region = 'north'", Seq(5), None, None),
        ("read_partition_null", "region IS NULL", Seq(3, 6), None, None),
        ("read_projection", null, rows.indices, None, Some(Seq("region", "value", "payload"))))
      for ((name, predicate, indices, version, columns) <- reads) {
        readSpec(t, name = Some(name), predicate = predicate,
          version = version.map(Long.box).orNull, columns = columns).assert { spec =>
          spec.expectation match {
            case Succeeded(result) => assert(result.rowCount == indices.size.toLong)
            case other => fail(s"$mode/$name must succeed: $other")
          }
          val literal = spark.createDataFrame(indices.map(rows).asJava, schema)
          val oracle = columns.map(cs => literal.selectExpr(cs: _*)).getOrElse(literal)
          val expected = outputDir.resolve(table.outputName).resolve("expected")
            .resolve(s"${table.outputName}_$name").resolve("expected_data")
          val captured = spark.read.parquet(expected.toString)
          RowComparison.assertRowsEqual(oracle, captured, s"$mode/$name literal bag")
          assert(captured.schema.fields.forall(_.nullable))
          println(s"Physical-name literal row/schema oracle passed: $mode/$name")
        }
      }
      for (version <- Seq(5L, 6L)) {
        snapshotSpec(t, version = version).assert { spec =>
          spec.expectation match {
            case Succeeded(result) =>
              assert(result.metadata.schemaString == metadata.get("schemaString").asText)
              assert(result.metadata.partitionColumns == Seq("region"))
              assert(result.metadata.configuration ==
                properties + ("delta.columnMapping.maxColumnId" -> "5"))
            case other => fail(s"$mode snapshot v$version must succeed: $other")
          }
          println(s"Physical-name snapshot mapping oracle passed: $mode/v$version")
        }
      }
    }
  }

  test("enable_name_mapping_with_full_rewrite") {
    sql("""CREATE TABLE tbl (id INT, value INT, payload STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'none',
        'delta.checkpointInterval' = '1000')""")
    sql("INSERT INTO tbl VALUES (1, 10, 'duplicate'), (1, 10, 'duplicate'), (2, NULL, 'nullable')")
    sql("INSERT INTO tbl VALUES (3, 30, NULL), (NULL, NULL, NULL), (NULL, NULL, NULL)")
    // A separately materialized source prevents replacement from reading its own target.
    // Both SQL-created tables are tracked by the normal WorkloadContext cleanup.
    sql("CREATE TABLE rewrite_source USING delta AS SELECT id, value, payload FROM tbl")
    sql("""CREATE OR REPLACE TABLE tbl USING delta TBLPROPERTIES (
      'delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2',
      'delta.minWriterVersion' = '5', 'delta.checkpointInterval' = '1000')
      AS SELECT id, value, payload FROM rewrite_source""")
    val t = registerTable("tbl")
    val table = WorkloadContext.current.getTableSpec(t)
    val before = (0L to 2L).flatMap(CommitLog.read(t.sourcePath, _))
    val rewrite = CommitLog.read(t.sourcePath, 3L)
    val oldAdds = before.collect { case add: AddFile => add }
    val newAdds = rewrite.collect { case add: AddFile => add }
    val removed = rewrite.collect { case remove: RemoveFile => remove }
    assert(!before.exists(_.isInstanceOf[RemoveFile]))
    assert(oldAdds.nonEmpty && newAdds.nonEmpty)
    val oldPaths = oldAdds.map(_.path).toSet
    val newPaths = newAdds.map(_.path).toSet
    assert(oldPaths.size == oldAdds.size && newPaths.size == newAdds.size)
    assert(removed.map(_.path).toSet == oldPaths && removed.size == oldPaths.size)
    assert(newPaths.intersect(oldPaths).isEmpty)
    assert((oldPaths -- removed.map(_.path)) ++ newPaths == newPaths)
    assert((oldAdds ++ newAdds).forall(add =>
      Files.isRegularFile(t.sourcePath.resolve(add.path)) &&
        Files.size(t.sourcePath.resolve(add.path)) == add.size))

    val schema = new StructType().add("id", IntegerType).add("value", IntegerType)
      .add("payload", StringType)
    val oldMetadata = before.collect { case metadata: LogMetadata => metadata }.last
    val newMetadata = rewrite.collect { case metadata: LogMetadata => metadata }.last
    assert(DataType.fromJson(oldMetadata.schemaString) == schema)
    assert(oldMetadata.configuration == Map(
      "delta.columnMapping.mode" -> "none", "delta.checkpointInterval" -> "1000"))
    val mapped = DataType.fromJson(newMetadata.schemaString).asInstanceOf[StructType]
    assert(mapped.fields.map(f => (f.name, f.dataType, f.nullable)).toSeq ==
      schema.fields.map(f => (f.name, f.dataType, f.nullable)).toSeq)
    val idKey = "delta.columnMapping.id"
    val physicalKey = "delta.columnMapping.physicalName"
    assert(mapped.fields.map(_.metadata.getLong(idKey)).toSet == Set(1L, 2L, 3L))
    val physicalNames = mapped.fields.map(_.metadata.getString(physicalKey)).toSeq
    assert(physicalNames.distinct.size == 3)
    assert(mapped.fields.forall(f => f.metadata.getString(physicalKey) != f.name))
    assert(newMetadata.configuration == Map(
      "delta.columnMapping.mode" -> "name", "delta.columnMapping.maxColumnId" -> "3",
      "delta.checkpointInterval" -> "1000"))
    assert(oldMetadata.partitionColumns.isEmpty && newMetadata.partitionColumns.isEmpty)
    assert(before.collect { case p: Protocol => p }.last == Protocol(1, 2))
    assert(rewrite.collect { case p: Protocol => p }.last == Protocol(2, 5))
    for (add <- oldAdds) {
      assert(spark.read.parquet(t.sourcePath.resolve(add.path).toString).schema == schema)
    }
    for (add <- newAdds) {
      val physical = spark.read.parquet(t.sourcePath.resolve(add.path).toString).schema
      assert(physical.fieldNames.toSeq == physicalNames)
      assert(physical.fields.map(_.dataType).toSeq == schema.fields.map(_.dataType).toSeq)
      assert(physical.fields.map(_.metadata.getLong("parquet.field.id")).toSeq ==
        mapped.fields.map(_.metadata.getLong(idKey)).toSeq)
    }
    println(s"Full-rewrite action/physical-name oracle passed: " +
      s"removed=${oldPaths.size}, fresh=${newPaths.size}")

    // Every selection is a literal bag, including both copies of each duplicate/null row.
    val rows = Seq(Row(1, 10, "duplicate"), Row(1, 10, "duplicate"),
      Row(2, null, "nullable"), Row(3, 30, null), Row(null, null, null), Row(null, null, null))
    val reads = Seq(
      ("read_before", null, rows.indices, Some(2L), None),
      ("read_all", null, rows.indices, None, None),
      ("read_rewritten", null, rows.indices, Some(3L), None),
      ("read_projection", null, rows.indices, None, Some(Seq("payload", "id"))),
      ("read_hit", "value = 10", Seq(0, 1), None, None),
      ("read_miss", "value = 999", Seq.empty[Int], None, None),
      ("read_null_value", "value IS NULL", Seq(2, 4, 5), None, None),
      ("read_null_payload", "payload IS NULL", Seq(3, 4, 5), None, None),
      ("read_null_id_projection", "id IS NULL", Seq(4, 5), None, Some(Seq("id", "value"))))
    for ((name, predicate, indices, version, columns) <- reads) {
      readSpec(t, name = Some(name), predicate = predicate,
        version = version.map(Long.box).orNull, columns = columns).assert { spec =>
        spec.expectation match {
          case Succeeded(result) => assert(result.rowCount == indices.size.toLong)
          case other => fail(s"full rewrite/$name must succeed: $other")
        }
        val literal = spark.createDataFrame(indices.map(rows).asJava, schema)
        val oracle = columns.map(cs => literal.selectExpr(cs: _*)).getOrElse(literal)
        val expected = outputDir.resolve(table.outputName).resolve("expected")
          .resolve(s"${table.outputName}_$name").resolve("expected_data")
        val captured = spark.read.parquet(expected.toString)
        RowComparison.assertRowsEqual(oracle, captured, s"full rewrite/$name literal bag")
        assert(captured.schema == oracle.schema)
        println(s"Full-rewrite literal row/schema oracle passed: $name")
      }
    }
    for ((version, metadata, reader, writer) <- Seq(
        (2L, oldMetadata, 1, 2), (3L, newMetadata, 2, 5))) {
      snapshotSpec(t, version = version).assert { spec =>
        spec.expectation match {
          case Succeeded(result) =>
            assert(result.metadata.schemaString == metadata.schemaString)
            assert(result.metadata.configuration == metadata.configuration)
            assert(result.metadata.partitionColumns.isEmpty)
            assert(result.protocol.minReaderVersion == reader)
            assert(result.protocol.minWriterVersion == writer)
          case other => fail(s"full rewrite snapshot v$version must succeed: $other")
        }
        println(s"Full-rewrite snapshot transition oracle passed: v$version")
      }
    }
  }

}
