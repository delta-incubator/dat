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

import org.apache.spark.sql.types.{DataType, StructType}

import io.delta.workload.{TableHandle, WorkloadContext, WorkloadTestSuite}
import io.delta.workload.engine.RowComparison
import io.delta.workload.json.JsonUtil
import io.delta.workload.log.AddFile
import io.delta.workload.model._

/**
 * Type widening workloads: numeric chains, float->double, decimal precision,
 * date->timestampNTZ, nested fields, arrays, maps, partitions, DVs,
 * column mapping, data skipping, projections, and null handling.
 */
class TypeWideningSuite extends WorkloadTestSuite("type_widening") {

  // Simple type widenings

  test("byte_to_int") {
    sql("""CREATE TABLE tbl (a BYTE) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    // v1: insert byte-range values
    sql("INSERT INTO tbl VALUES (CAST(1 AS BYTE)), (CAST(127 AS BYTE))")
    // v2: widen byte -> short
    sql("ALTER TABLE tbl ALTER COLUMN a TYPE SHORT")
    // v3: insert short-range values
    sql("INSERT INTO tbl VALUES (CAST(128 AS SHORT)), (CAST(32767 AS SHORT))")
    // v4: widen short -> int
    sql("ALTER TABLE tbl ALTER COLUMN a TYPE INT")
    // v5: insert int-range values
    sql("INSERT INTO tbl VALUES (32768), (2147483647)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "a > 127", name = Some("read_gt_max_byte"))
    readSpec(t, predicate = "a > 32767", name = Some("read_gt_max_short"))
    for (v <- 0L to 5L) snapshotSpec(t, version = v)
  }

  test("short_to_int") {
    sql("""CREATE TABLE tbl (id INT, value SHORT) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(100 AS SHORT)), (2, CAST(32767 AS SHORT))")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    sql("INSERT INTO tbl VALUES (3, 40000), (4, 100000)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value > 32767", name = Some("read_gt_max_short"))
    snapshotSpec(t)
  }

  test("short_to_long") {
    sql("""CREATE TABLE tbl (id INT, value SHORT) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(100 AS SHORT)), (2, CAST(32767 AS SHORT))")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    sql("INSERT INTO tbl VALUES (3, 40000), (4, 2147483647)")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    sql("INSERT INTO tbl VALUES (5, 3000000000L), (6, 9223372036854775807L)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value > 32767", name = Some("read_gt_max_short"))
    readSpec(t, predicate = "value > 2147483647", name = Some("read_gt_max_int"))
    for (v <- 0L to 5L) snapshotSpec(t, version = v)
  }

  test("int_to_long") {
    sql("""CREATE TABLE tbl (a INT) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1), (2147483647)")
    sql("ALTER TABLE tbl ALTER COLUMN a TYPE LONG")
    sql("INSERT INTO tbl VALUES (2147483648L), (9223372036854775807L)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "a > 2147483647", name = Some("read_gt_max_int"))
    readSpec(t, version = 1, name = Some("read_v1_before_widening"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("full_numeric_chain") {
    sql("""CREATE TABLE tbl (a BYTE) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (CAST(1 AS BYTE)), (CAST(100 AS BYTE))")
    sql("ALTER TABLE tbl ALTER COLUMN a TYPE SHORT")
    sql("INSERT INTO tbl VALUES (CAST(200 AS SHORT)), (CAST(30000 AS SHORT))")
    sql("ALTER TABLE tbl ALTER COLUMN a TYPE INT")
    sql("INSERT INTO tbl VALUES (40000), (2000000000)")
    sql("ALTER TABLE tbl ALTER COLUMN a TYPE LONG")
    sql("INSERT INTO tbl VALUES (3000000000L), (9000000000000000000L)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_v1_byte_only"))
    readSpec(t, version = 3, name = Some("read_v3_through_short"))
    readSpec(t, version = 5, name = Some("read_v5_through_int"))
    for (v <- 0L to 7L) snapshotSpec(t, version = v)
  }

  test("float_to_double") {
    sql("""CREATE TABLE tbl (value FLOAT) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (CAST(1.5 AS FLOAT)), (CAST(3.14 AS FLOAT))")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE DOUBLE")
    sql("INSERT INTO tbl VALUES (3.141592653589793), (1.7976931348623157E308)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value > 100.0", name = Some("read_high_precision"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("decimal_precision") {
    sql("""CREATE TABLE tbl (amount DECIMAL(5,2)) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (123.45), (999.99)")
    sql("ALTER TABLE tbl ALTER COLUMN amount TYPE DECIMAL(10,2)")
    sql("INSERT INTO tbl VALUES (12345678.90), (99999999.99)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "amount > 1000", name = Some("read_large_values"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("cross_physical_decimal") {
    sql("""CREATE TABLE tbl (amount DECIMAL(9,2)) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (123.45), (9999999.99)")
    sql("ALTER TABLE tbl ALTER COLUMN amount TYPE DECIMAL(18,2)")
    sql("INSERT INTO tbl VALUES (1234567890123.45), (9999999999999999.99)")
    sql("ALTER TABLE tbl ALTER COLUMN amount TYPE DECIMAL(28,3)")
    sql("INSERT INTO tbl VALUES (1234567890123456789012345.678)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_v1_int32_only"))
    readSpec(t, version = 3, name = Some("read_v3_through_int64"))
    for (v <- 0L to 5L) snapshotSpec(t, version = v)
  }

  test("date_to_timestamp_ntz") {
    sql("""CREATE TABLE tbl (a DATE) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true',
        'delta.feature.timestampNtz' = 'supported')""")
    sql("INSERT INTO tbl VALUES (DATE'2024-01-15'), (DATE'2024-06-30')")
    sql("ALTER TABLE tbl ALTER COLUMN a TYPE TIMESTAMP_NTZ")
    sql("INSERT INTO tbl VALUES (TIMESTAMP_NTZ'2024-12-31 23:59:59'), (TIMESTAMP_NTZ'2025-01-01 12:30:00')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_v1_before_widening"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("nested_field") {
    sql("""CREATE TABLE tbl (data STRUCT<id: INT, count: INT>) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (named_struct('id', 1, 'count', 100))")
    sql("INSERT INTO tbl VALUES (named_struct('id', 2, 'count', 2000000000))")
    sql("ALTER TABLE tbl ALTER COLUMN data.count TYPE LONG")
    sql("INSERT INTO tbl VALUES (named_struct('id', 3, 'count', 3000000000L))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "data.count > 2147483647", name = Some("read_large_count"))
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("array_element") {
    sql("""CREATE TABLE tbl (values ARRAY<INT>) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (array(1, 2, 3))")
    sql("INSERT INTO tbl VALUES (array(100, 200, 2147483647))")
    sql("ALTER TABLE tbl ALTER COLUMN values.element TYPE LONG")
    sql("INSERT INTO tbl VALUES (array(3000000000L, 9000000000000L))")
    val t = registerTable("tbl")
    readSpec(t)
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("map_key_value_widening") {
    sql("""CREATE TABLE tbl (
      s STRUCT<a: BYTE>,
      m MAP<BYTE, SHORT>,
      a ARRAY<BYTE>
    ) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (named_struct('a', CAST(1 AS BYTE)), map(CAST(1 AS BYTE), CAST(10 AS SHORT)), array(CAST(1 AS BYTE), CAST(2 AS BYTE)))")
    // Widen map key: byte -> int
    sql("ALTER TABLE tbl ALTER COLUMN m.key TYPE INT")
    // Widen map value: short -> int
    sql("ALTER TABLE tbl ALTER COLUMN m.value TYPE INT")
    // Widen array element: byte -> int
    sql("ALTER TABLE tbl ALTER COLUMN a.element TYPE INT")
    // Widen struct field: byte -> int
    sql("ALTER TABLE tbl ALTER COLUMN s.a TYPE INT")
    sql("INSERT INTO tbl VALUES (named_struct('a', 50000), map(50000, 100000), array(50000, 60000))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_v1_before_widening"))
    for (v <- 0L to 6L) snapshotSpec(t, version = v)
  }

  test("with_dv") {
    sql("""CREATE TABLE tbl (id INT, value SHORT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableTypeWidening' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT)), (3, CAST(30 AS SHORT)), (4, CAST(40 AS SHORT)), (5, CAST(50 AS SHORT))")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    sql("INSERT INTO tbl VALUES (6, 40000), (7, 50000)")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value > 32767", name = Some("read_wide_values"))
    snapshotSpec(t)
  }

  test("with_partition") {
    sql("""CREATE TABLE tbl (id INT, value SHORT, category STRING) USING delta
      PARTITIONED BY (category) TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT), 'A'), (2, CAST(20 AS SHORT), 'B'), (3, CAST(30 AS SHORT), 'A')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    sql("INSERT INTO tbl VALUES (4, 40000, 'A'), (5, 50000, 'B')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "category = 'A'", name = Some("read_partition_A"))
    readSpec(t, predicate = "value > 32767", name = Some("read_wide_values"))
    snapshotSpec(t)
  }

  test("with_column_mapping") {
    sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableTypeWidening' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, 100), (2, 2000000000)")
    // Rename column first
    sql("ALTER TABLE tbl RENAME COLUMN value TO score")
    // Then widen
    sql("ALTER TABLE tbl ALTER COLUMN score TYPE LONG")
    sql("INSERT INTO tbl VALUES (3, 3000000000L), (4, 9000000000000L)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "score > 2147483647", name = Some("filter_on_widened"))
    readSpec(t, columns = Some(Seq("id", "score")), name = Some("project_renamed_widened"))
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("colmap_rename") {
    sql("""CREATE TABLE tbl (id INT, val SHORT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableTypeWidening' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(32767 AS SHORT))")
    // Widen first, then rename
    sql("ALTER TABLE tbl ALTER COLUMN val TYPE INT")
    sql("ALTER TABLE tbl RENAME COLUMN val TO amount")
    sql("INSERT INTO tbl VALUES (3, 40000), (4, 100000)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "amount > 32767", name = Some("filter_wide"))
    readSpec(t, columns = Some(Seq("id", "amount")), name = Some("project_renamed"))
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("change_tracking_across_widening") {
    sql("""CREATE TABLE tbl (id INT, amount SHORT) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableTypeWidening' = 'true',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(100 AS SHORT)), (2, CAST(200 AS SHORT))")
    sql("ALTER TABLE tbl ALTER COLUMN amount TYPE INT")
    sql("INSERT INTO tbl VALUES (3, 40000), (4, 50000)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_original"))
    snapshotSpec(t)
  }

  test("row_tracking_combo") {
    sql("""CREATE TABLE tbl (id INT, score SHORT) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true',
        'spark.delta.properties.defaults.enableRowTracking' = 'true',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT))")
    sql("ALTER TABLE tbl ALTER COLUMN score TYPE INT")
    sql("INSERT INTO tbl VALUES (3, 40000), (4, 50000)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "score > 32767", name = Some("read_wide_values"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("with_data_skipping") {
    sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 100), (2, 200)")
    sql("INSERT INTO tbl VALUES (3, 300), (4, 400)")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    sql("INSERT INTO tbl VALUES (5, 3000000000L), (6, 9000000000000L)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value > 2147483647", name = Some("read_large_values_only"))
    readSpec(t, predicate = "value > 150 AND value < 350", name = Some("read_with_predicate_on_widened"))
    snapshotSpec(t)
  }

  test("stats_after_change") {
    sql("""CREATE TABLE tbl (id INT, metric SHORT) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(100 AS SHORT))")
    sql("ALTER TABLE tbl ALTER COLUMN metric TYPE INT")
    sql("INSERT INTO tbl VALUES (3, 40000), (4, 50000)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "metric <= 100", name = Some("predicate_old_range"))
    readSpec(t, predicate = "metric > 32767", name = Some("predicate_new_range"))
    readSpec(t, predicate = "metric >= 100 AND metric <= 40000", name = Some("predicate_cross_range"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("project_widened") {
    sql("""CREATE TABLE tbl (id INT, value INT, label STRING) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 100, 'a'), (2, 200, 'b')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    sql("INSERT INTO tbl VALUES (3, 3000000000L, 'c'), (4, 4000000000L, 'd')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("value")), name = Some("project_widened_only"))
    readSpec(t, columns = Some(Seq("id", "value")), name = Some("project_widened_with_id"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("project_non_widened") {
    sql("""CREATE TABLE tbl (id INT, value INT, label STRING) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 100, 'a'), (2, 200, 'b')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    sql("INSERT INTO tbl VALUES (3, 3000000000L, 'c'), (4, 4000000000L, 'd')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "label")), name = Some("project_non_widened"))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("null_handling") {
    sql("""CREATE TABLE tbl (id INT, value SHORT) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(NULL AS SHORT)), (3, CAST(30 AS SHORT)), (4, CAST(NULL AS SHORT))")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    sql("INSERT INTO tbl VALUES (5, 40000), (6, CAST(NULL AS INT))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value IS NOT NULL", name = Some("read_non_null"))
    readSpec(t, predicate = "value IS NULL", name = Some("read_nulls_only"))
    snapshotSpec(t)
  }

  // Widened JSON statistics, old-type parsed checkpoint statistics, and partition values
  // are separate workloads per transition.
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
