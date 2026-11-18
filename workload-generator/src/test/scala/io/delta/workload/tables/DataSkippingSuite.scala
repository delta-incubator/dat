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

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{Files, Path, StandardCopyOption}
import java.sql.Timestamp
import java.time.{Instant, LocalDate}

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.{NanoTime, SimpleGroupFactory}
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetReader}
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupReadSupport}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DecimalType, IntegerType,
  StringType, StructType, TimestampNTZType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import io.delta.workload.{TableHandle, WorkloadContext, WorkloadTestSuite}
import io.delta.workload.deltaharness.{DeltaHarness, Format => HarnessFormat}
import io.delta.workload.engine.{RowComparison, SnapshotResolver}
import io.delta.workload.json.JsonUtil
import io.delta.workload.log.{Action, AddFile, CommitLog, DomainMetadata, Metadata, Protocol,
  RemoveFile, Txn}
import io.delta.workload.model._
import io.delta.workload.tables.DeletionVectorPayloads.payload

/**
 * Data skipping, statistics, and partitioning workloads.
 * Covers: equality, range, IN, IS NULL, BETWEEN, LIKE, NOT, AND/OR combinations,
 * nested field predicates, boolean predicates, typed stats, null handling,
 * multiple files for skipping, missing stats, long strings, column mapping stats,
 * generated columns, DVs, partitioned skipping, partition pruning, projection,
 * stats edge cases (null min/max, numRecords-only, truncated strings), predicate row
 * semantics (nondeterminism, signed zero, casts), partial/tight/selected statistics,
 * timestamp statistic encodings and time zones, stats refresh, and clustering rewrites.
 */
class DataSkippingSuite extends WorkloadTestSuite("data_skipping")
    with ScopedSessionConf with LiteralOracleChecks {

  // === Data Skipping ===

  // Top-level single value: all comparison operators

  test("top_level_single_1") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    // hits
    readSpec(t, predicate = "a = 1")
    readSpec(t, predicate = "a >= 1")
    readSpec(t, predicate = "a <= 1")
    readSpec(t, predicate = "a >= 0")
    readSpec(t, predicate = "a <= 2")
    readSpec(t, predicate = "0 <= a")
    readSpec(t, predicate = "1 <= a")
    readSpec(t, predicate = "1 >= a")
    readSpec(t, predicate = "2 >= a")
    readSpec(t, predicate = "1 = a")
    readSpec(t, predicate = "a <=> 1")
    readSpec(t, predicate = "1 <=> a")
    readSpec(t, predicate = "NOT (a <=> 2)", name = Some("read_not_a_nse_2"))
    readSpec(t, predicate = "true", name = Some("read_true"))
    // misses
    readSpec(t, predicate = "NOT (a = 1)", name = Some("read_miss_not_a_eq_1"))
    readSpec(t, predicate = "NOT (a <=> 1)", name = Some("read_miss_not_a_nse_1"))
    readSpec(t, predicate = "a = 2", name = Some("read_miss_a_eq_2"))
    readSpec(t, predicate = "a <=> 2", name = Some("read_miss_a_nse_2"))
    readSpec(t, predicate = "a > 1", name = Some("read_miss_a_gt_1"))
    readSpec(t, predicate = "a >= 2", name = Some("read_miss_a_gte_2"))
    readSpec(t, predicate = "a <= 0", name = Some("read_miss_a_lte_0"))
    readSpec(t, predicate = "a = 0", name = Some("read_miss_a_eq_0"))
    readSpec(t, predicate = "a > 2", name = Some("read_miss_a_gt_2"))
    readSpec(t, predicate = "a < 1", name = Some("read_miss_a_lt_1"))
    readSpec(t, predicate = "a <> 1", name = Some("read_miss_a_neq_1"))
    readSpec(t, predicate = "1 != a", name = Some("read_miss_1_neq_a"))
    readSpec(t, predicate = "2 <=> a", name = Some("read_miss_2_nse_a"))
    readSpec(t, predicate = "0 >= a", name = Some("read_miss_0_gte_a"))
    readSpec(t, predicate = "0 = a", name = Some("read_miss_0_eq_a"))
    readSpec(t, predicate = "1 > a", name = Some("read_miss_1_gt_a"))
    readSpec(t, predicate = "1 < a", name = Some("read_miss_1_lt_a"))
    readSpec(t, predicate = "0 > a", name = Some("read_miss_0_gt_a"))
    readSpec(t, predicate = "2 = a", name = Some("read_miss_2_eq_a"))
    readSpec(t, predicate = "2 <= a", name = Some("read_miss_2_lte_a"))
    readSpec(t, predicate = "0 < a AND a < 1", name = Some("read_miss_between_0_1"))
    snapshotSpec(t)
  }

  test("nested_single_1") {
    sql("CREATE TABLE tbl (a STRUCT<b: LONG>) USING delta")
    sql("INSERT INTO tbl VALUES (named_struct('b', 1))")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a.b = 1")
    readSpec(t, predicate = "a.b >= 0")
    readSpec(t, predicate = "a.b >= 1")
    readSpec(t, predicate = "a.b <= 1")
    readSpec(t, predicate = "a.b <= 2")
    readSpec(t, predicate = "a.b = 2", name = Some("read_miss_ab_eq_2"))
    readSpec(t, predicate = "a.b > 1", name = Some("read_miss_ab_gt_1"))
    readSpec(t, predicate = "a.b < 1", name = Some("read_miss_ab_lt_1"))
    snapshotSpec(t)
  }

  test("double_nested_single_1") {
    sql("CREATE TABLE tbl (a STRUCT<b: STRUCT<c: LONG>>) USING delta")
    sql("INSERT INTO tbl VALUES (named_struct('b', named_struct('c', 1)))")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a.b.c = 1")
    readSpec(t, predicate = "a.b.c >= 0")
    readSpec(t, predicate = "a.b.c >= 1")
    readSpec(t, predicate = "a.b.c <= 1")
    readSpec(t, predicate = "a.b.c <= 2")
    readSpec(t, predicate = "a.b.c = 2", name = Some("read_miss_abc_eq_2"))
    readSpec(t, predicate = "a.b.c > 1", name = Some("read_miss_abc_gt_1"))
    readSpec(t, predicate = "a.b.c < 1", name = Some("read_miss_abc_lt_1"))
    snapshotSpec(t)
  }

  test("nested_struct_predicate") {
    sql("CREATE TABLE tbl (id INT, info STRUCT<score: INT, name: STRING>) USING delta")
    sql("INSERT INTO tbl VALUES (1, named_struct('score', 90, 'name', 'alice'))")
    sql("INSERT INTO tbl VALUES (2, named_struct('score', 50, 'name', 'bob'))")
    val t = registerTable("tbl")
    readSpec(t, predicate = "info.score > 80")
    readSpec(t, predicate = "info.score < 40", name = Some("read_miss_low_score"))
    snapshotSpec(t)
  }

  test("complex_nested") {
    sql("CREATE TABLE tbl (a INT, b STRUCT<x: INT, y: INT>) USING delta")
    sql("INSERT INTO tbl VALUES (1, named_struct('x', 10, 'y', 20))")
    sql("INSERT INTO tbl VALUES (2, named_struct('x', 30, 'y', 40))")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 1 AND b.x = 10")
    readSpec(t, predicate = "b.x > 20 OR b.y < 25")
    readSpec(t, predicate = "a > 5 AND b.x > 50", name = Some("read_miss_complex"))
    snapshotSpec(t)
  }

  test("and_simple") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a <= 1 AND a > -1", name = Some("read_hit_and_bound"))
    readSpec(t, predicate = "a >= 1 AND a <= 2", name = Some("read_hit_and_range"))
    readSpec(t, predicate = "a > 5 AND a < 10", name = Some("read_miss_and_outside"))
    snapshotSpec(t)
  }

  test("and_two_fields") {
    sql("CREATE TABLE tbl (a LONG, b LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1, 10), (2, 20)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 1 AND b = 10")
    readSpec(t, predicate = "a >= 1 AND b <= 20")
    readSpec(t, predicate = "a = 1 AND b > 100", name = Some("read_miss_b_out"))
    readSpec(t, predicate = "a > 5 AND b > 5", name = Some("read_miss_both_out"))
    snapshotSpec(t)
  }

  test("and_one_side_unsupported") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 1 AND CAST(a AS STRING) LIKE '%1'")
    readSpec(t, predicate = "a > 5 AND CAST(a AS STRING) LIKE '%x'", name = Some("read_miss_and_unsupported"))
    snapshotSpec(t)
  }

  test("or_simple") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 1 OR a = 3")
    readSpec(t, predicate = "a < 0 OR a > 0")
    readSpec(t, predicate = "a = 5 OR a = 6", name = Some("read_miss_or"))
    snapshotSpec(t)
  }

  test("or_two_fields") {
    sql("CREATE TABLE tbl (a LONG, b LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1, 10), (2, 20)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 1 OR b = 20")
    readSpec(t, predicate = "a = 5 OR b = 10")
    readSpec(t, predicate = "a > 0 OR b > 0")
    readSpec(t, predicate = "a = 5 OR b = 50", name = Some("read_miss_or_both"))
    snapshotSpec(t)
  }

  test("or_one_side_unsupported") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    // OR with unsupported side forces full scan
    readSpec(t, predicate = "a = 1 OR CAST(a AS STRING) LIKE '%x'")
    readSpec(t, predicate = "a > 5 OR CAST(a AS STRING) LIKE '%1'")
    snapshotSpec(t)
  }

  test("not_simple") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "NOT (a > 5)")
    readSpec(t, predicate = "NOT (a < 0)", name = Some("read_not_lt_0"))
    snapshotSpec(t)
  }

  test("not_and") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "NOT (a > 5 AND a < 10)")
    readSpec(t, predicate = "NOT (a > 0 AND a < 3)")
    readSpec(t, predicate = "NOT (a = 1 AND a = 2)", name = Some("read_not_and_contra"))
    snapshotSpec(t)
  }

  test("not_or") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "NOT (a > 5 OR a < -5)")
    readSpec(t, predicate = "NOT (a = 1 OR a = 2)", name = Some("read_not_or_all"))
    snapshotSpec(t)
  }

  test("starts_with") {
    sql("CREATE TABLE tbl (a STRING) USING delta")
    sql("INSERT INTO tbl VALUES ('apple'), ('banana')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a LIKE 'a%'")
    readSpec(t, predicate = "a LIKE 'b%'")
    readSpec(t, predicate = "a LIKE 'app%'")
    readSpec(t, predicate = "a LIKE 'z%'", name = Some("read_miss_z"))
    readSpec(t, predicate = "a LIKE 'c%'", name = Some("read_miss_c"))
    snapshotSpec(t)
  }

  test("starts_with_nested") {
    sql("CREATE TABLE tbl (a STRUCT<b: STRING>) USING delta")
    sql("INSERT INTO tbl VALUES (named_struct('b', 'apple'))")
    sql("INSERT INTO tbl VALUES (named_struct('b', 'banana'))")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a.b LIKE 'a%'")
    readSpec(t, predicate = "a.b LIKE 'b%'")
    readSpec(t, predicate = "a.b LIKE 'app%'")
    readSpec(t, predicate = "a.b LIKE 'z%'", name = Some("read_miss_z"))
    readSpec(t, predicate = "a.b LIKE 'c%'", name = Some("read_miss_c"))
    snapshotSpec(t)
  }

  test("string_patterns") {
    sql("CREATE TABLE tbl (name STRING) USING delta")
    sql("INSERT INTO tbl VALUES ('alice'), ('bob')")
    sql("INSERT INTO tbl VALUES ('charlie'), ('diana')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "name = 'alice'")
    readSpec(t, predicate = "name >= 'c'")
    readSpec(t, predicate = "name < 'b'")
    readSpec(t, predicate = "name LIKE 'a%'")
    readSpec(t, predicate = "name LIKE 'ch%'")
    readSpec(t, predicate = "name LIKE 'z%'", name = Some("read_miss_z"))
    readSpec(t, predicate = "name > 'e'", name = Some("read_miss_gt_e"))
    readSpec(t, predicate = "name LIKE 'x%'", name = Some("read_miss_x"))
    snapshotSpec(t)
  }

  test("long_strings_min") {
    sql("CREATE TABLE tbl (a STRING) USING delta")
    // 33-char prefix: "aaa...a" then differ
    val longA = "a" * 32 + "x"
    val longB = "a" * 32 + "y"
    sql(s"INSERT INTO tbl VALUES ('$longA'), ('$longB')")
    val t = registerTable("tbl")
    readSpec(t, predicate = s"a = '$longA'")
    readSpec(t, predicate = s"a >= '${"a" * 32}'")
    readSpec(t, predicate = "a LIKE 'aaa%'")
    readSpec(t, predicate = "a = 'z'", name = Some("read_miss_z"))
    readSpec(t, predicate = "a < 'a'", name = Some("read_miss_lt_a"))
    snapshotSpec(t)
  }

  test("long_strings_max") {
    sql("CREATE TABLE tbl (a STRING) USING delta")
    val longZ = "z" * 32 + "a"
    val longY = "z" * 32 + "b"
    sql(s"INSERT INTO tbl VALUES ('$longZ'), ('$longY')")
    val t = registerTable("tbl")
    readSpec(t, predicate = s"a = '$longZ'")
    readSpec(t, predicate = s"a >= '${"z" * 32}'")
    readSpec(t, predicate = "a LIKE 'zzz%'")
    readSpec(t, predicate = s"a <= '${"z" * 33}'")
    readSpec(t, predicate = "a = 'a'", name = Some("read_miss_a"))
    readSpec(t, predicate = "a < 'z'", name = Some("read_miss_lt_z"))
    readSpec(t, predicate = "a > 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'", name = Some("read_miss_gt_long_z"))
    snapshotSpec(t)
  }

  test("in_set") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a IN (1, 2)")
    readSpec(t, predicate = "a IN (3)")
    readSpec(t, predicate = "a IN (10, 20)", name = Some("read_miss_in"))
    snapshotSpec(t)
  }

  test("in_list") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (10), (20)")
    sql("INSERT INTO tbl VALUES (30), (40)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a IN (10, 30)")
    readSpec(t, predicate = "a IN (99)", name = Some("read_miss_in_99"))
    snapshotSpec(t)
  }

  test("in_nested") {
    sql("CREATE TABLE tbl (s STRUCT<x: INT>) USING delta")
    sql("INSERT INTO tbl VALUES (named_struct('x', 1))")
    sql("INSERT INTO tbl VALUES (named_struct('x', 5))")
    val t = registerTable("tbl")
    readSpec(t, predicate = "s.x IN (1, 5)")
    readSpec(t, predicate = "s.x IN (99)", name = Some("read_miss_nested_in"))
    snapshotSpec(t)
  }

  test("in_with_nulls_mixed") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a IN (1, NULL)")
    readSpec(t, predicate = "a IN (99, NULL)", name = Some("read_in_null_miss"))
    snapshotSpec(t)
  }

  test("in_with_nulls_only") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (NULL), (NULL)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a IN (1)")
    readSpec(t, predicate = "a IN (NULL)")
    snapshotSpec(t)
  }

  test("in_with_thresholds") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    val t = registerTable("tbl")
    // Small IN list
    readSpec(t, predicate = "a IN (1, 2, 3)")
    // Larger IN list (may exceed threshold and become range)
    readSpec(t, predicate = "a IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)",
      name = Some("read_in_large"))
    readSpec(t, predicate = "a IN (100, 200)", name = Some("read_miss_in_large"))
    snapshotSpec(t)
  }

  test("not_in") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a NOT IN (4, 5)")
    readSpec(t, predicate = "a NOT IN (1, 2, 3)", name = Some("read_not_in_all"))
    readSpec(t, predicate = "a NOT IN (1)")
    snapshotSpec(t)
  }

  test("is_null") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a IS NULL")
    snapshotSpec(t)
  }

  test("is_not_null") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a IS NOT NULL")
    snapshotSpec(t)
  }

  test("isnull_complex_expr") {
    sql("CREATE TABLE tbl (a INT, b STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'x'), (NULL, NULL)")
    sql("INSERT INTO tbl VALUES (3, 'y'), (NULL, 'z')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a IS NULL AND b IS NULL")
    readSpec(t, predicate = "a IS NULL OR b IS NULL")
    readSpec(t, predicate = "a IS NOT NULL AND b IS NOT NULL")
    snapshotSpec(t)
  }

  test("nulls_only_null") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (NULL)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a IS NULL")
    readSpec(t, predicate = "a IS NOT NULL", name = Some("read_is_not_null"))
    readSpec(t, predicate = "a = 1", name = Some("read_eq_1"))
    readSpec(t, predicate = "a > 0", name = Some("read_gt_0"))
    readSpec(t, predicate = "a < 0", name = Some("read_lt_0"))
    readSpec(t, predicate = "a >= 0", name = Some("read_gte_0"))
    readSpec(t, predicate = "a <= 0", name = Some("read_lte_0"))
    readSpec(t, predicate = "a <=> NULL", name = Some("read_nse_null"))
    readSpec(t, predicate = "a <=> 1", name = Some("read_nse_1"))
    readSpec(t, predicate = "a IN (1, 2)", name = Some("read_in_1_2"))
    readSpec(t, predicate = "NOT (a = 1)", name = Some("read_not_eq_1"))
    readSpec(t, predicate = "NOT (a IS NULL)", name = Some("read_not_is_null"))
    readSpec(t, predicate = "NOT (a IS NOT NULL)", name = Some("read_not_is_not_null"))
    readSpec(t, predicate = "a = 1 OR a IS NULL", name = Some("read_eq_or_null"))
    readSpec(t, predicate = "a = 1 AND a IS NULL", name = Some("read_eq_and_null"))
    readSpec(t, predicate = "a LIKE 'x%'", name = Some("read_like_x"))
    snapshotSpec(t)
  }

  test("nulls_only_nonnull") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a IS NULL", name = Some("read_is_null"))
    readSpec(t, predicate = "a IS NOT NULL")
    snapshotSpec(t)
  }

  test("nulls_mixed") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a IS NULL")
    readSpec(t, predicate = "a IS NOT NULL")
    readSpec(t, predicate = "a = 1")
    readSpec(t, predicate = "a > 2")
    readSpec(t, predicate = "a < 2")
    readSpec(t, predicate = "a >= 1")
    readSpec(t, predicate = "a <= 3")
    readSpec(t, predicate = "a <=> NULL", name = Some("read_nse_null"))
    readSpec(t, predicate = "a <=> 1", name = Some("read_nse_1"))
    readSpec(t, predicate = "a IN (1, 3)")
    readSpec(t, predicate = "a IN (5)", name = Some("read_in_miss_5"))
    readSpec(t, predicate = "a = 1 OR a IS NULL")
    readSpec(t, predicate = "a = 1 AND a IS NOT NULL")
    readSpec(t, predicate = "NOT (a = 1)")
    readSpec(t, predicate = "NOT (a IS NULL)", name = Some("read_not_is_null"))
    readSpec(t, predicate = "a > 5", name = Some("read_miss_gt_5"))
    readSpec(t, predicate = "a < 0", name = Some("read_miss_lt_0"))
    snapshotSpec(t)
  }

  test("nulls_nonnulls_only") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a IS NULL")
    readSpec(t, predicate = "a IS NOT NULL")
    readSpec(t, predicate = "a = 2")
    readSpec(t, predicate = "a > 5", name = Some("read_miss_gt_5"))
    snapshotSpec(t)
  }

  test("nulls_partial_stats") {
    sql("""CREATE TABLE tbl (a LONG, b STRING) USING delta
      TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1')""")
    sql("INSERT INTO tbl VALUES (1, 'x'), (2, 'y'), (3, 'z')")
    val t = registerTable("tbl")
    // a has stats, b does not
    readSpec(t, predicate = "a = 1")
    readSpec(t, predicate = "a > 5", name = Some("read_miss_a_gt_5"))
    readSpec(t, predicate = "b = 'x'")
    readSpec(t, predicate = "b = 'nonexistent'", name = Some("read_b_no_stats"))
    readSpec(t, predicate = "a = 1 AND b = 'x'")
    readSpec(t, predicate = "a > 5 AND b = 'x'", name = Some("read_miss_a_has_stats"))
    readSpec(t, predicate = "a = 1 OR b = 'nonexistent'")
    readSpec(t, predicate = "a IS NULL", name = Some("read_a_is_null"))
    snapshotSpec(t)
  }

  test("null_safe_eq") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a <=> 1")
    readSpec(t, predicate = "a <=> NULL")
    readSpec(t, predicate = "a <=> 3")
    readSpec(t, predicate = "a <=> 99", name = Some("read_miss_nse_99"))
    snapshotSpec(t)
  }

  test("null_string_partition") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, NULL), (3, 'b')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "part IS NULL")
    readSpec(t, predicate = "part = 'a'")
    snapshotSpec(t)
  }

  test("null_mixed_partitions") {
    sql("""CREATE TABLE tbl (id INT, p1 STRING, p2 INT) USING delta
      PARTITIONED BY (p1, p2)""")
    sql("INSERT INTO tbl VALUES (1, 'a', 1), (2, NULL, 2), (3, 'b', NULL), (4, NULL, NULL)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "p1 IS NULL")
    readSpec(t, predicate = "p2 IS NULL")
    readSpec(t, predicate = "p1 IS NULL AND p2 IS NULL")
    snapshotSpec(t)
  }

  test("between") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (5), (10)")
    sql("INSERT INTO tbl VALUES (15), (20), (25)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a BETWEEN 1 AND 10")
    readSpec(t, predicate = "a BETWEEN 5 AND 20")
    readSpec(t, predicate = "a BETWEEN 1 AND 25", name = Some("read_between_all"))
    readSpec(t, predicate = "a BETWEEN 50 AND 100", name = Some("read_miss_between"))
    snapshotSpec(t)
  }

  test("boolean") {
    sql("CREATE TABLE tbl (a BOOLEAN) USING delta")
    sql("INSERT INTO tbl VALUES (true)")
    sql("INSERT INTO tbl VALUES (false)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = true")
    readSpec(t, predicate = "a = false")
    readSpec(t, predicate = "a IS NOT NULL")
    readSpec(t, predicate = "a IS NULL", name = Some("read_miss_null"))
    snapshotSpec(t)
  }

  test("boolean_column") {
    sql("CREATE TABLE tbl (id INT, active BOOLEAN) USING delta")
    sql("INSERT INTO tbl VALUES (1, true), (2, true)")
    sql("INSERT INTO tbl VALUES (3, false), (4, false)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "active = true")
    readSpec(t, predicate = "active = false")
    snapshotSpec(t)
  }

  test("numeric_types") {
    sql("CREATE TABLE tbl (i INT, l LONG, f FLOAT, d DOUBLE) USING delta")
    sql("INSERT INTO tbl VALUES (1, 100, 1.5, 2.5)")
    sql("INSERT INTO tbl VALUES (10, 1000, 10.5, 20.5)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "i = 1")
    readSpec(t, predicate = "l > 500")
    readSpec(t, predicate = "f < 2.0")
    readSpec(t, predicate = "d >= 20.0")
    readSpec(t, predicate = "i > 100", name = Some("read_miss_i"))
    readSpec(t, predicate = "d < 1.0", name = Some("read_miss_d"))
    snapshotSpec(t)
  }

  test("tinyint_smallint") {
    sql("CREATE TABLE tbl (t TINYINT, s SMALLINT) USING delta")
    sql("INSERT INTO tbl VALUES (1, 100)")
    sql("INSERT INTO tbl VALUES (127, 32767)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "t = 1")
    readSpec(t, predicate = "s > 30000")
    readSpec(t, predicate = "t > 127", name = Some("read_miss_t"))
    snapshotSpec(t)
  }

  test("float_special_values") {
    sql("CREATE TABLE tbl (f FLOAT) USING delta")
    sql("INSERT INTO tbl VALUES (CAST('NaN' AS FLOAT)), (CAST('Infinity' AS FLOAT)), (CAST('-0.0' AS FLOAT))")
    val t = registerTable("tbl")
    readSpec(t, predicate = "f > 0")
    readSpec(t, predicate = "f IS NOT NULL")
    snapshotSpec(t)
  }

  test("binary_type") {
    sql("CREATE TABLE tbl (id INT, data BINARY) USING delta")
    sql("INSERT INTO tbl VALUES (1, X'0102'), (2, NULL)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "data IS NOT NULL")
    readSpec(t, predicate = "data IS NULL")
    snapshotSpec(t)
  }

  test("implicit_cast") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    // int literal vs long column
    readSpec(t, predicate = "a = 1")
    readSpec(t, predicate = "a > 0")
    snapshotSpec(t)
  }

  test("datetime") {
    sql("CREATE TABLE tbl (d DATE, ts TIMESTAMP) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'2024-01-01', TIMESTAMP'2024-01-01 00:00:00')")
    sql("INSERT INTO tbl VALUES (DATE'2024-06-15', TIMESTAMP'2024-06-15 12:00:00')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "d = DATE'2024-01-01'")
    readSpec(t, predicate = "d > DATE'2024-03-01'")
    readSpec(t, predicate = "ts < TIMESTAMP'2024-03-01 00:00:00'")
    readSpec(t, predicate = "d > DATE'2025-01-01'", name = Some("read_miss_future"))
    snapshotSpec(t)
  }

  test("timestamp_microsecond") {
    sql("CREATE TABLE tbl (ts TIMESTAMP) USING delta")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-01-01 00:00:00.000001')")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-01-01 00:00:00.000002')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "ts = TIMESTAMP'2024-01-01 00:00:00.000001'")
    readSpec(t, predicate = "ts > TIMESTAMP'2024-01-01 00:00:00.000002'",
      name = Some("read_miss_after"))
    snapshotSpec(t)
  }

  test("timestamp_ntz_skipping") {
    sql("""CREATE TABLE tbl (ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7',
        'delta.feature.timestampNtz' = 'supported')""")
    sql("INSERT INTO tbl VALUES (TIMESTAMP_NTZ'2024-01-01 00:00:00')")
    sql("INSERT INTO tbl VALUES (TIMESTAMP_NTZ'2024-06-15 12:00:00')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "ts = TIMESTAMP_NTZ'2024-01-01 00:00:00'")
    readSpec(t, predicate = "ts > TIMESTAMP_NTZ'2025-01-01 00:00:00'",
      name = Some("read_miss_future_ntz"))
    snapshotSpec(t)
  }

  test("year_function") {
    sql("CREATE TABLE tbl (d DATE, value INT) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'2024-03-15', 1), (DATE'2024-11-20', 2)")
    sql("INSERT INTO tbl VALUES (DATE'2025-01-05', 3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "year(d) = 2024")
    readSpec(t, predicate = "year(d) = 2025")
    readSpec(t, predicate = "year(d) = 2020", name = Some("read_miss_year"))
    snapshotSpec(t)
  }

  test("month_function") {
    sql("CREATE TABLE tbl (d DATE, value INT) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'2024-01-15', 1), (DATE'2024-06-20', 2)")
    sql("INSERT INTO tbl VALUES (DATE'2024-12-05', 3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "month(d) = 1")
    readSpec(t, predicate = "month(d) = 6")
    readSpec(t, predicate = "month(d) = 8", name = Some("read_miss_month"))
    snapshotSpec(t)
  }

  test("trunc_date") {
    sql("CREATE TABLE tbl (d DATE) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'2024-03-15'), (DATE'2024-03-20')")
    sql("INSERT INTO tbl VALUES (DATE'2024-06-01'), (DATE'2024-06-30')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "trunc(d, 'MONTH') = DATE'2024-03-01'")
    readSpec(t, predicate = "trunc(d, 'MONTH') = DATE'2024-06-01'")
    readSpec(t, predicate = "trunc(d, 'YEAR') = DATE'2025-01-01'", name = Some("read_miss_trunc"))
    snapshotSpec(t)
  }

  test("date_trunc_timestamp") {
    sql("CREATE TABLE tbl (ts TIMESTAMP) USING delta")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-03-15 10:30:00')")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-06-01 14:00:00')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "date_trunc('MONTH', ts) = TIMESTAMP'2024-03-01 00:00:00'")
    readSpec(t, predicate = "date_trunc('YEAR', ts) = TIMESTAMP'2025-01-01 00:00:00'",
      name = Some("read_miss_trunc_ts"))
    snapshotSpec(t)
  }

  test("datediff") {
    sql("CREATE TABLE tbl (d DATE) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'2024-01-01'), (DATE'2024-01-10')")
    sql("INSERT INTO tbl VALUES (DATE'2024-06-01')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "datediff(d, DATE'2024-01-01') <= 10")
    readSpec(t, predicate = "datediff(d, DATE'2024-01-01') > 100")
    snapshotSpec(t)
  }

  test("date_add_sub") {
    sql("CREATE TABLE tbl (d DATE) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'2024-01-01'), (DATE'2024-01-15')")
    sql("INSERT INTO tbl VALUES (DATE'2024-06-01')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "d >= date_add(DATE'2024-01-01', -1)")
    readSpec(t, predicate = "d <= date_sub(DATE'2024-01-01', 10)",
      name = Some("read_miss_date_sub"))
    snapshotSpec(t)
  }

  test("multi_file_ranges") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")   // file 1: 1-10
    sql("INSERT INTO tbl SELECT id FROM range(11, 21)")  // file 2: 11-20
    sql("INSERT INTO tbl SELECT id FROM range(21, 31)")  // file 3: 21-30
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_full_scan"))
    readSpec(t, predicate = "a <= 10", name = Some("read_hit_file1_only"))
    readSpec(t, predicate = "a > 10 AND a <= 20", name = Some("read_hit_file2_only"))
    readSpec(t, predicate = "a > 20", name = Some("read_hit_file3_only"))
    readSpec(t, predicate = "a <= 15", name = Some("read_hit_file1_and_2"))
    readSpec(t, predicate = "a > 15", name = Some("read_hit_file2_and_3"))
    readSpec(t, predicate = "a > 100", name = Some("read_miss_all_gt_100"))
    readSpec(t, predicate = "a < 0", name = Some("read_miss_all_lt_0"))
    snapshotSpec(t)
  }

  test("multi_file_time") {
    sql("CREATE TABLE tbl (ts TIMESTAMP, value INT) USING delta")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-01-01 00:00:00', 1), (TIMESTAMP'2024-01-15 00:00:00', 2)")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-06-01 00:00:00', 3), (TIMESTAMP'2024-12-31 00:00:00', 4)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "ts < TIMESTAMP'2024-02-01 00:00:00'")
    readSpec(t, predicate = "ts >= TIMESTAMP'2024-06-01 00:00:00'")
    readSpec(t, predicate = "ts > TIMESTAMP'2025-01-01 00:00:00'", name = Some("read_miss_future"))
    snapshotSpec(t)
  }

  test("typed_stats") {
    sql("""CREATE TABLE tbl (
      c1 LONG, c2 STRING, c3 FLOAT, c4 DOUBLE,
      c5 TIMESTAMP, c6 TIMESTAMP_NTZ, c7 DATE,
      c8 BYTE, c9 SHORT, c10 DECIMAL(3,2)
    ) USING delta
    TBLPROPERTIES ('delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7',
      'delta.feature.timestampNtz' = 'supported')""")
    sql("""INSERT INTO tbl VALUES
      (1, 'abc', 1.5, 2.5, TIMESTAMP'2024-01-01 00:00:00',
       TIMESTAMP_NTZ'2024-01-01 00:00:00', DATE'2024-01-01', 1, 100, 1.23)""")
    sql("""INSERT INTO tbl VALUES
      (100, 'xyz', 99.9, 199.9, TIMESTAMP'2024-12-31 23:59:59',
       TIMESTAMP_NTZ'2024-12-31 23:59:59', DATE'2024-12-31', 127, 32000, 9.87)""")
    val t = registerTable("tbl")
    readSpec(t, predicate = "c1 = 1")
    readSpec(t, predicate = "c1 > 50")
    readSpec(t, predicate = "c2 = 'abc'")
    readSpec(t, predicate = "c3 < 2.0")
    readSpec(t, predicate = "c4 >= 100.0")
    readSpec(t, predicate = "c5 = TIMESTAMP'2024-01-01 00:00:00'")
    readSpec(t, predicate = "c6 > TIMESTAMP_NTZ'2024-06-01 00:00:00'")
    readSpec(t, predicate = "c7 = DATE'2024-01-01'")
    readSpec(t, predicate = "c7 > DATE'2024-06-01'")
    readSpec(t, predicate = "c8 = 1")
    readSpec(t, predicate = "c9 > 20000")
    readSpec(t, predicate = "c10 > 5.00")
    readSpec(t, predicate = "c10 = 1.23")
    readSpec(t, predicate = "c1 > 200", name = Some("read_miss_c1"))
    readSpec(t, predicate = "c3 > 200.0", name = Some("read_miss_c3"))
    readSpec(t, predicate = "c4 < 1.0", name = Some("read_miss_c4"))
    readSpec(t, predicate = "c5 > TIMESTAMP'2025-06-01 00:00:00'", name = Some("read_miss_c5"))
    readSpec(t, predicate = "c7 > DATE'2025-01-01'", name = Some("read_miss_c7"))
    readSpec(t, predicate = "c10 > 9.99", name = Some("read_miss_c10"))
    snapshotSpec(t)
  }

  test("variant_null_stats") {
    sql("""CREATE TABLE tbl (
      v VARIANT, v_struct STRUCT<v: VARIANT>,
      null_v VARIANT, null_v_struct STRUCT<v: VARIANT>
    ) USING delta
    TBLPROPERTIES ('delta.feature.variantType-preview' = 'supported')""")
    sql("""INSERT INTO tbl VALUES (
      PARSE_JSON('1'), named_struct('v', PARSE_JSON('"hello"')),
      NULL, named_struct('v', NULL))""")
    val t = registerTable("tbl")
    readSpec(t, predicate = "v IS NOT NULL")
    readSpec(t, predicate = "v IS NULL")
    readSpec(t, predicate = "null_v IS NULL")
    readSpec(t, predicate = "null_v IS NOT NULL")
    readSpec(t, predicate = "v_struct.v IS NOT NULL")
    readSpec(t, predicate = "v_struct.v IS NULL")
    readSpec(t, predicate = "null_v_struct.v IS NULL")
    snapshotSpec(t)
  }

  test("indexed_names_empty") {
    sql("""CREATE TABLE tbl (a LONG, b LONG) USING delta
      TBLPROPERTIES ('delta.dataSkippingStatsColumns' = '')""")
    sql("INSERT INTO tbl VALUES (1, 10), (2, 20)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 1")
    readSpec(t, predicate = "b = 10")
    readSpec(t, predicate = "a > 100", name = Some("read_a_gt_100"))
    snapshotSpec(t)
  }

  test("indexed_names_subset") {
    sql("""CREATE TABLE tbl (a LONG, b LONG, c LONG) USING delta
      TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'a,c')""")
    sql("INSERT INTO tbl VALUES (1, 10, 100)")
    sql("INSERT INTO tbl VALUES (5, 50, 500)")
    val t = registerTable("tbl")
    // a has stats
    readSpec(t, predicate = "a = 1")
    readSpec(t, predicate = "a > 10", name = Some("read_miss_a"))
    // b has no stats
    readSpec(t, predicate = "b = 10")
    readSpec(t, predicate = "b > 100", name = Some("read_b_no_skip"))
    // c has stats
    readSpec(t, predicate = "c = 100")
    readSpec(t, predicate = "c > 1000", name = Some("read_miss_c"))
    readSpec(t, predicate = "a = 1 AND c = 100")
    readSpec(t, predicate = "a > 10 AND c > 1000", name = Some("read_miss_ac"))
    readSpec(t, predicate = "a = 1 OR b = 10")
    snapshotSpec(t)
  }

  test("indexed_names_nested") {
    sql("""CREATE TABLE tbl (
      a STRUCT<x: LONG, y: LONG>,
      b STRUCT<p: LONG, q: LONG>
    ) USING delta
    TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'a.x,b.q')""")
    sql("INSERT INTO tbl VALUES (named_struct('x',1,'y',10), named_struct('p',100,'q',1000))")
    sql("INSERT INTO tbl VALUES (named_struct('x',5,'y',50), named_struct('p',500,'q',5000))")
    val t = registerTable("tbl")
    // a.x has stats
    readSpec(t, predicate = "a.x = 1")
    readSpec(t, predicate = "a.x > 10", name = Some("read_miss_ax"))
    // a.y no stats
    readSpec(t, predicate = "a.y = 10")
    readSpec(t, predicate = "a.y > 100", name = Some("read_ay_no_skip"))
    // b.p no stats
    readSpec(t, predicate = "b.p = 100")
    // b.q has stats
    readSpec(t, predicate = "b.q = 1000")
    readSpec(t, predicate = "b.q > 10000", name = Some("read_miss_bq"))
    readSpec(t, predicate = "a.x = 1 AND b.q = 1000")
    readSpec(t, predicate = "a.x > 10 AND b.q > 10000", name = Some("read_miss_ax_bq"))
    snapshotSpec(t)
  }

  test("indexed_names_complex") {
    sql("""CREATE TABLE tbl (
      a STRUCT<x: LONG, y: STRUCT<z: LONG>>,
      b LONG
    ) USING delta
    TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'a.x,a.y.z,b')""")
    sql("INSERT INTO tbl VALUES (named_struct('x',1,'y',named_struct('z',10)), 100)")
    sql("INSERT INTO tbl VALUES (named_struct('x',5,'y',named_struct('z',50)), 500)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a.x = 1")
    readSpec(t, predicate = "a.y.z = 10")
    readSpec(t, predicate = "b = 100")
    readSpec(t, predicate = "a.x > 10", name = Some("read_miss_ax"))
    readSpec(t, predicate = "a.y.z > 100", name = Some("read_miss_ayz"))
    snapshotSpec(t)
  }

  test("more_cols_than_indexed") {
    sql("""CREATE TABLE tbl (a LONG, b LONG, c LONG, d LONG) USING delta
      TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '2')""")
    sql("INSERT INTO tbl VALUES (1, 10, 100, 1000)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 1")
    readSpec(t, predicate = "b = 10")
    readSpec(t, predicate = "c = 100")  // no stats
    readSpec(t, predicate = "d = 1000")  // no stats
    snapshotSpec(t)
  }

  test("missing_stats_cols") {
    sql("""CREATE TABLE tbl (a LONG) USING delta
      TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1')""")
    sql("INSERT INTO tbl VALUES (1), (2)")
    sql("ALTER TABLE tbl ADD COLUMN b LONG")
    sql("INSERT INTO tbl VALUES (3, 30)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 1")
    readSpec(t, predicate = "b = 30")
    readSpec(t, predicate = "a > 5", name = Some("read_miss_a"))
    readSpec(t, predicate = "b IS NULL")
    snapshotSpec(t)
  }

  test("missing_stats_graceful") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = registerTable("tbl")
    // Strip stats from commit
    modifyCommitActions(t, version = 1) { actions =>
      actions.map {
        case a: AddFile => a.copy(stats = None)
        case other => other
      }
    }
    readSpec(t, predicate = "a = 1")
    readSpec(t, predicate = "a > 99")
    snapshotSpec(t)
  }

  test("stats_config_change") {
    sql("""CREATE TABLE tbl (a INT, b INT) USING delta
      TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '2')""")
    sql("INSERT INTO tbl VALUES (1, 10)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1')")
    sql("INSERT INTO tbl VALUES (2, 20)")
    val t = registerTable("tbl")
    // First file: both a and b have stats; second file: only a
    readSpec(t, predicate = "a = 1")
    readSpec(t, predicate = "b = 10")
    snapshotSpec(t)
  }

  test("nested_indexed_0") {
    sql("""CREATE TABLE tbl (a STRUCT<x: LONG, y: LONG>, b LONG) USING delta
      TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '0')""")
    sql("INSERT INTO tbl VALUES (named_struct('x',1,'y',10), 100)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a.x = 1")
    readSpec(t, predicate = "a.y = 10")
    readSpec(t, predicate = "b = 100")
    readSpec(t, predicate = "b > 500", name = Some("read_b_no_stats"))
    snapshotSpec(t)
  }

  test("nested_indexed_3") {
    sql("""CREATE TABLE tbl (
      a STRUCT<x: LONG, y: LONG>,
      b LONG,
      c STRUCT<p: LONG>
    ) USING delta
    TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '3')""")
    sql("INSERT INTO tbl VALUES (named_struct('x',1,'y',10), 100, named_struct('p',1000))")
    sql("INSERT INTO tbl VALUES (named_struct('x',5,'y',50), 500, named_struct('p',5000))")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a.x = 1")
    readSpec(t, predicate = "a.y = 10")
    readSpec(t, predicate = "b = 100")
    readSpec(t, predicate = "c.p = 1000")
    readSpec(t, predicate = "a.x > 10", name = Some("read_miss_ax"))
    readSpec(t, predicate = "b > 1000", name = Some("read_miss_b"))
    readSpec(t, predicate = "a.x = 1 AND b = 100")
    readSpec(t, predicate = "a.x = 1 OR c.p = 5000")
    snapshotSpec(t)
  }

  test("nested_indexed_6") {
    sql("""CREATE TABLE tbl (
      a STRUCT<x: LONG, y: LONG, z: LONG>,
      b STRUCT<p: LONG, q: LONG>,
      c LONG
    ) USING delta
    TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '6')""")
    sql("INSERT INTO tbl VALUES (named_struct('x',1,'y',2,'z',3), named_struct('p',4,'q',5), 6)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a.x = 1")
    readSpec(t, predicate = "a.z = 3")
    readSpec(t, predicate = "b.p = 4")
    readSpec(t, predicate = "b.q = 5")
    readSpec(t, predicate = "c = 6")
    snapshotSpec(t)
  }

  test("nested_indexed_9") {
    sql("""CREATE TABLE tbl (
      a STRUCT<x: LONG, y: LONG, z: LONG>,
      b STRUCT<p: LONG, q: LONG, r: LONG>,
      c STRUCT<s: LONG, t: LONG, u: LONG>
    ) USING delta
    TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '9')""")
    sql("""INSERT INTO tbl VALUES (
      named_struct('x',1,'y',2,'z',3),
      named_struct('p',4,'q',5,'r',6),
      named_struct('s',7,'t',8,'u',9))""")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a.x = 1")
    readSpec(t, predicate = "b.r = 6")
    readSpec(t, predicate = "c.u = 9")
    readSpec(t, predicate = "c.u > 100", name = Some("read_miss_cu"))
    readSpec(t, predicate = "a.x = 1 AND c.u = 9")
    snapshotSpec(t)
  }

  test("partitioned") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'a')")
    sql("INSERT INTO tbl VALUES (3, 'b'), (4, 'b')")
    sql("INSERT INTO tbl VALUES (5, 'c')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "part = 'a'")
    readSpec(t, predicate = "part = 'b' AND id > 3")
    readSpec(t, predicate = "id < 3")
    readSpec(t, predicate = "part = 'z'", name = Some("read_miss_part"))
    readSpec(t, predicate = "id > 100", name = Some("read_miss_id"))
    snapshotSpec(t)
  }

  test("partition_and_stats") {
    sql("CREATE TABLE tbl (id INT, value INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 10, 'a'), (2, 20, 'a')")
    sql("INSERT INTO tbl VALUES (3, 30, 'b'), (4, 40, 'b')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "part = 'a' AND value > 15")
    readSpec(t, predicate = "part = 'b' AND value < 25", name = Some("read_miss_combined"))
    readSpec(t, predicate = "part = 'a' OR value > 35")
    snapshotSpec(t)
  }

  test("partition_or_predicate") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "part = 'a' OR part = 'c'")
    readSpec(t, predicate = "part = 'z'", name = Some("read_miss_part"))
    snapshotSpec(t)
  }

  test("schema_order_mismatch") {
    sql("CREATE TABLE tbl (a INT, b INT, c INT) USING delta")
    sql("INSERT INTO tbl VALUES (1, 2, 3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "c = 3")
    readSpec(t, columns = Some(Seq("c", "a")))
    snapshotSpec(t)
  }

  test("nonexistent_col_filter") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 1")
    // nonexistent column handled gracefully
    snapshotSpec(t)
  }

  test("with_dvs_edge") {
    sql("""CREATE TABLE tbl (a INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")   // file 1: 1-10
    sql("INSERT INTO tbl SELECT id FROM range(11, 21)")  // file 2: 11-20
    sql("DELETE FROM tbl WHERE a = 5")  // DV on file 1
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 5", name = Some("read_deleted_row"))
    readSpec(t, predicate = "a > 15")
    readSpec(t, predicate = "a = 1")
    snapshotSpec(t)
  }

  test("with_dvs_edge_1") {
    sql("""CREATE TABLE tbl (a INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    sql("INSERT INTO tbl SELECT id FROM range(11, 21)")
    sql("DELETE FROM tbl WHERE a <= 5")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a > 10")
    readSpec(t, predicate = "a <= 5", name = Some("read_all_deleted"))
    snapshotSpec(t)
  }

  test("with_dvs_edge_2") {
    sql("""CREATE TABLE tbl (a INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
    sql("DELETE FROM tbl WHERE a <= 5")  // all of file 1 deleted
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "a > 5")
    snapshotSpec(t)
  }

  test("stats_col_drop") {
    sql("""CREATE TABLE tbl (a INT, b INT, c INT) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 10, 100)")
    sql("ALTER TABLE tbl DROP COLUMN b")
    sql("INSERT INTO tbl VALUES (2, 200)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 1")
    readSpec(t, predicate = "c = 100")
    snapshotSpec(t)
  }

  test("stats_col_rename") {
    sql("""CREATE TABLE tbl (a INT, old_name INT) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 100)")
    sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    sql("INSERT INTO tbl VALUES (2, 200)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 1")
    readSpec(t, predicate = "new_name > 150")
    snapshotSpec(t)
  }

  test("stats_after_drop") {
    sql("""CREATE TABLE tbl (
      c1 LONG, c2 STRING, c3 FLOAT, c4 DOUBLE,
      c5 TIMESTAMP, c6 TIMESTAMP_NTZ, c7 DATE,
      c8 BYTE, c9 SHORT, c10 DECIMAL(3,2)
    ) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7',
      'delta.feature.timestampNtz' = 'supported')""")
    sql("""INSERT INTO tbl VALUES (1, 'a', 1.5, 2.5,
      TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP_NTZ'2024-01-01 00:00:00',
      DATE'2024-01-01', 1, 100, 1.23)""")
    sql("ALTER TABLE tbl DROP COLUMN c2")
    sql("ALTER TABLE tbl DROP COLUMN c8")
    sql("ALTER TABLE tbl DROP COLUMN c9")
    sql("""INSERT INTO tbl VALUES (2, 3.0, 4.5,
      TIMESTAMP'2024-06-15 00:00:00', TIMESTAMP_NTZ'2024-06-15 00:00:00',
      DATE'2024-06-15', 2.34)""")
    val t = registerTable("tbl")
    readSpec(t, predicate = "c1 = 1")
    readSpec(t, predicate = "c1 > 1")
    readSpec(t, predicate = "c3 < 2.0")
    readSpec(t, predicate = "c4 > 3.0")
    readSpec(t, predicate = "c5 = TIMESTAMP'2024-01-01 00:00:00'")
    readSpec(t, predicate = "c6 > TIMESTAMP_NTZ'2024-03-01 00:00:00'")
    readSpec(t, predicate = "c7 = DATE'2024-01-01'")
    readSpec(t, predicate = "c10 > 2.00")
    readSpec(t, predicate = "c1 > 100", name = Some("read_miss_c1"))
    readSpec(t, predicate = "c3 > 100.0", name = Some("read_miss_c3"))
    readSpec(t, predicate = "c7 > DATE'2025-01-01'", name = Some("read_miss_c7"))
    readSpec(t, predicate = "c10 > 9.99", name = Some("read_miss_c10"))
    readSpec(t, predicate = "c1 = 1 AND c10 = 1.23")
    snapshotSpec(t)
  }

  test("stats_after_rename") {
    sql("""CREATE TABLE tbl (
      c1 LONG, c2 STRING, c3 FLOAT, c4 DOUBLE,
      c5 TIMESTAMP, c6 TIMESTAMP_NTZ, c7 DATE,
      c8 BYTE, c9 SHORT, c10 DECIMAL(3,2)
    ) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7',
      'delta.feature.timestampNtz' = 'supported')""")
    sql("""INSERT INTO tbl VALUES (1, 'a', 1.5, 2.5,
      TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP_NTZ'2024-01-01 00:00:00',
      DATE'2024-01-01', 1, 100, 1.23)""")
    sql("ALTER TABLE tbl RENAME COLUMN c2 TO renamed_c2")
    sql("ALTER TABLE tbl RENAME COLUMN c8 TO renamed_c8")
    sql("""INSERT INTO tbl VALUES (2, 'b', 3.0, 4.5,
      TIMESTAMP'2024-06-15 00:00:00', TIMESTAMP_NTZ'2024-06-15 00:00:00',
      DATE'2024-06-15', 2, 200, 2.34)""")
    val t = registerTable("tbl")
    readSpec(t, predicate = "c1 = 1")
    readSpec(t, predicate = "c1 > 1")
    readSpec(t, predicate = "renamed_c2 = 'a'")
    readSpec(t, predicate = "c3 < 2.0")
    readSpec(t, predicate = "c4 > 3.0")
    readSpec(t, predicate = "c5 = TIMESTAMP'2024-01-01 00:00:00'")
    readSpec(t, predicate = "c6 > TIMESTAMP_NTZ'2024-03-01 00:00:00'")
    readSpec(t, predicate = "c7 = DATE'2024-01-01'")
    readSpec(t, predicate = "renamed_c8 = 1")
    readSpec(t, predicate = "c9 > 150")
    readSpec(t, predicate = "c10 > 2.00")
    readSpec(t, predicate = "c1 > 100", name = Some("read_miss_c1"))
    readSpec(t, predicate = "renamed_c2 = 'z'", name = Some("read_miss_renamed_c2"))
    readSpec(t, predicate = "c3 > 100.0", name = Some("read_miss_c3"))
    readSpec(t, predicate = "c7 > DATE'2025-01-01'", name = Some("read_miss_c7"))
    readSpec(t, predicate = "c10 > 9.99", name = Some("read_miss_c10"))
    readSpec(t, predicate = "c1 = 1 AND renamed_c2 = 'a'")
    readSpec(t, predicate = "c1 = 1 AND c10 = 1.23")
    readSpec(t, predicate = "c1 > 100 AND c10 > 9.99", name = Some("read_miss_c1_c10"))
    snapshotSpec(t)
  }

  test("err_001_field_not_found") {
    sql("""CREATE TABLE tbl (a INT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("stats_null_in_min_max") {
    sql("""CREATE TABLE tbl (id INT, nullable_col STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, null),(2, null),(3, null)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "nullable_col = 'a'")
    readSpec(t, predicate = "nullable_col IS NULL")
    snapshotSpec(t)
  }

  test("stats_numrecords_only") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'a'),(2, 'b')")
    val t = registerTable("tbl")
    // Strip min/max stats, keep only numRecords
    modifyCommitActions(t, 0) { actions =>
      actions.map {
        case a: AddFile if a.stats.exists(_.contains("numRecords")) =>
          val statsNode = JsonUtil.mapper.readTree(a.stats.get)
          val newStats = JsonUtil.mapper.createObjectNode()
          newStats.set[com.fasterxml.jackson.databind.JsonNode](
            "numRecords", statsNode.get("numRecords"))
          a.copy(stats = Some(JsonUtil.mapper.writeValueAsString(newStats)))
        case other => other
      }
    }
    snapshotSpec(t)
  }

  test("stats_numrecords_with_dv") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("DELETE FROM tbl WHERE id < 3")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id >= 5")
    snapshotSpec(t)
  }

  test("stats_partition_col_no_stats") {
    sql("""CREATE TABLE tbl (id INT, country STRING, amount INT) USING delta
      PARTITIONED BY (country) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'US', 100),(2, 'UK', 200),(3, 'US', 300),(4, 'UK', 400)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "country = 'US'")
    readSpec(t, predicate = "amount > 200")
    snapshotSpec(t)
  }

  test("stats_string_truncation") {
    sql("""CREATE TABLE tbl (id INT, long_str STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // Include a string longer than 32 chars to trigger truncation
    sql("""INSERT INTO tbl VALUES
      (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaxyz'),
      (2, 'short'),
      (3, 'medium_length_string')""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "long_str = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaxyz'")
    readSpec(t, predicate = "long_str = 'short'")
    snapshotSpec(t)
  }

  test("stats_empty_string") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, ''),(2, 'a'),(3, '')")
    val t = registerTable("tbl")
    // Strip stats completely to simulate empty stats string
    modifyCommitActions(t, 0) { actions =>
      actions.map {
        case a: AddFile if a.stats.isDefined => a.copy(stats = Some(""))
        case other => other
      }
    }
    snapshotSpec(t)
  }

  test("stats_missing_entirely") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'a'),(2, 'b')")
    val t = registerTable("tbl")
    // Remove stats field entirely
    modifyCommitActions(t, 0) { actions =>
      actions.map {
        case a: AddFile => a.copy(stats = None)
        case other => other
      }
    }
    snapshotSpec(t)
  }

  test("single_partition") {
    sql("""CREATE TABLE tbl (id INT, region STRING, value DOUBLE)
      USING delta PARTITIONED BY (region)""")
    sql("INSERT INTO tbl VALUES (1,'us',10),(2,'us',20),(3,'eu',30),(4,'eu',40),(5,'asia',50)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "region = 'us'")
    readSpec(t, predicate = "region = 'eu'")
    readSpec(t, predicate = "region = 'antarctica'")
    snapshotSpec(t)
  }

  test("multi_partition") {
    sql("""CREATE TABLE tbl (id INT, year INT, month INT, data STRING)
      USING delta PARTITIONED BY (year, month)""")
    sql("""INSERT INTO tbl VALUES
      (1,2024,1,'jan24'),(2,2024,2,'feb24'),(3,2024,3,'mar24'),
      (4,2025,1,'jan25'),(5,2025,2,'feb25')""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "year = 2024")
    readSpec(t, predicate = "year = 2025 AND month = 1")
    readSpec(t, columns = Some(Seq("id", "year")))
    snapshotSpec(t)
  }

  test("null_partition") {
    sql("""CREATE TABLE tbl (id INT, category STRING, value INT)
      USING delta PARTITIONED BY (category)""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,NULL,20),(3,'b',30),(4,NULL,40)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "category IS NULL")
    readSpec(t, predicate = "category IS NOT NULL")
    readSpec(t, predicate = "category = 'a'")
    snapshotSpec(t)
  }

  test("stats_skipping") {
    sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    sql("INSERT INTO tbl SELECT id, id * 10 FROM range(100) WHERE id < 50")
    sql("INSERT INTO tbl SELECT id, id * 10 FROM range(100) WHERE id >= 50")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id < 25")
    readSpec(t, predicate = "id >= 75")
    readSpec(t, predicate = "id > 999")
    readSpec(t, predicate = "id >= 0")
    snapshotSpec(t)
  }

  test("partition_pruning") {
    sql("""CREATE TABLE tbl (id INT, region STRING, amount DOUBLE)
      USING delta PARTITIONED BY (region)""")
    sql("INSERT INTO tbl VALUES (1,'us',10),(2,'us',20)")
    sql("INSERT INTO tbl VALUES (3,'eu',30),(4,'eu',40)")
    sql("INSERT INTO tbl VALUES (5,'asia',50)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "region = 'us'")
    readSpec(t, predicate = "region = 'us' AND amount > 15")
    readSpec(t, predicate = "region = 'mars'")
    snapshotSpec(t)
  }

  test("column_projection") {
    sql("CREATE TABLE tbl (a INT, b STRING, c DOUBLE, d BOOLEAN, e DATE) USING delta")
    sql("""INSERT INTO tbl VALUES
      (1,'x',1.1,true,DATE'2024-01-01'),
      (2,'y',2.2,false,DATE'2024-06-15'),
      (3,'z',3.3,true,DATE'2025-01-01')""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("a")))
    readSpec(t, columns = Some(Seq("b", "d")))
    readSpec(t, columns = Some(Seq("e", "c", "a")))
    readSpec(t, predicate = "a > 1", columns = Some(Seq("a", "b")))
    snapshotSpec(t)
  }

  test("part_date_type") {
    sql("""CREATE TABLE tbl (id INT, value STRING, dt DATE) USING delta
      PARTITIONED BY (dt)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1,'jan',DATE'2024-01-01'),(2,'jun',DATE'2024-06-01'),
      (3,'dec',DATE'2024-12-01'),(4,'jan2',DATE'2024-01-01')""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "dt = DATE'2024-01-01'")
    readSpec(t, predicate = "dt >= DATE'2024-06-01'")
    snapshotSpec(t)
  }

  test("part_null_values") {
    sql("""CREATE TABLE tbl (id INT, value STRING, part STRING) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a','x'),(2,'b',NULL),(3,'c','y'),(4,'d',NULL)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "part IS NULL")
    readSpec(t, predicate = "part IS NOT NULL")
    snapshotSpec(t)
  }

  test("part_or_predicate") {
    sql("""CREATE TABLE tbl (id INT, value STRING, part STRING) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a1','A'),(2,'a2','A'),(3,'b1','B'),(4,'c1','C'),(5,'c2','C')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "part = 'A' OR part = 'C'")
    readSpec(t, predicate = "part = 'B'")
    snapshotSpec(t)
  }

  test("part_multi_column") {
    sql("""CREATE TABLE tbl (id INT, value STRING, a INT, b STRING, c BOOLEAN) USING delta
      PARTITIONED BY (a, b, c)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1,'v1',1,'x',true),(2,'v2',1,'y',false),
      (3,'v3',2,'y',true),(4,'v4',2,'z',false),
      (5,'v5',3,'z',true)""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "a = 1")
    readSpec(t, predicate = "a = 2 AND b = 'y'")
    readSpec(t, predicate = "a = 3 AND b = 'z' AND c = true")
    snapshotSpec(t)
  }

  // Row semantics for read predicates, without a pruning assertion: nondeterministic
  // values, signed zero, explicit casts at precision boundaries and narrowing wrap-around,
  // and complex filters. Signed zero needs Parquet filter pushdown disabled and the three
  // narrowing casts need ANSI disabled; both are scoped to those tests. Use `startswith`;
  // `starts_with` is unsupported.

  for (partitioned <- Seq(false, true)) {
    val layout = if (partitioned) "partitioned" else "unpartitioned"
    test(s"nondeterministic_values_$layout") {
      val partition = if (partitioned) "PARTITIONED BY (p)" else ""
      sql(s"CREATE TABLE tbl (id INT, value INT, p INT) USING delta $partition")
      sql("INSERT INTO tbl VALUES (1, 10, 1), (2, 20, 1), (2, 20, 1)")
      sql("INSERT INTO tbl VALUES (3, 20, 2), (4, 30, 2), (5, 40, 3)")
      sql("INSERT INTO tbl VALUES (6, NULL, 1), (7, 50, NULL), (8, NULL, NULL)")
      val t = registerTable("tbl")
      val table = WorkloadContext.current.getTableSpec(t)

      // Literal bags are independent of the filtered Delta reads, including duplicate/NULL rows.
      val all = Seq(
        "(1, 10, 1)",
        "(2, 20, 1)",
        "(2, 20, 1)",
        "(3, 20, 2)",
        "(4, 30, 2)",
        "(5, 40, 3)",
        "(6, NULL, 1)",
        "(7, 50, NULL)",
        "(8, NULL, NULL)"
      )
      val equality = Seq("(2, 20, 1)", "(2, 20, 1)", "(3, 20, 2)")
      val range = equality :+ "(4, 30, 2)"
      val partitionBag = Seq("(1, 10, 1)", "(2, 20, 1)", "(2, 20, 1)", "(6, NULL, 1)")
      val cases = Seq(
        ("read_all", null, all),
        ("random_all", "rand() >= 0", all),
        ("random_none", "rand() < 0", Seq.empty[String]),
        ("equality_control", "value = 20", equality),
        ("range_control", "value >= 20 AND value < 40", range),
        ("and_equality", "rand() >= 0 AND value = 20", equality),
        ("and_range", "rand() >= 0 AND (value >= 20 AND value < 40)", range),
        ("and_none", "rand() < 0 AND value = 20", Seq.empty[String]),
        ("or_equality", "rand() < 0 OR value = 20", equality),
        ("or_range", "rand() < 0 OR (value >= 20 AND value < 40)", range),
        ("or_all", "rand() >= 0 OR value = 20", all),
        ("partition_control", "p = 1", partitionBag),
        ("and_partition", "p = 1 AND rand() >= 0", partitionBag),
        ("or_partition", "p = 1 OR rand() < 0", partitionBag),
        // Spark can simplify the domain-only controls. Referencing the row's small exact INT id
        // keeps a nondeterministic expression at read time: adding [0, 1) cannot decrease id.
        ("row_random_all", "rand() + id >= id", all),
        ("row_random_none", "rand() + id < id", Seq.empty[String]),
        ("row_and_equality", "rand() + id >= id AND value = 20", equality),
        ("row_and_range", "rand() + id >= id AND (value >= 20 AND value < 40)", range),
        ("row_or_equality", "rand() + id < id OR value = 20", equality),
        ("row_or_range", "rand() + id < id OR (value >= 20 AND value < 40)", range),
        ("row_and_partition", "p = 1 AND rand() + id >= id", partitionBag),
        ("row_or_partition", "p = 1 OR rand() + id < id", partitionBag)
      )

      // rand() stays in the serialized read predicate. Its [0, 1) domain gives a stable bag
      // across RNG seeds, partition counts and replays; random values are never stored on INSERT.
      for ((label, predicate, bag) <- cases) {
        readSpec(t, predicate = predicate, name = Some(label)).assert { spec =>
          spec.expectation match {
            case Succeeded(_) =>
            case other => fail(s"${table.outputName}/$label must succeed: $other")
          }
          val oracle = if (bag.nonEmpty) {
            s"SELECT CAST(id AS INT) AS id, CAST(value AS INT) AS value, " +
            s"CAST(p AS INT) AS p FROM VALUES ${bag.mkString(", ")} AS expected(id, value, p)"
          } else {
            "SELECT CAST(NULL AS INT) AS id, CAST(NULL AS INT) AS value, " +
            "CAST(NULL AS INT) AS p WHERE false"
          }
          val expectedPath = outputDir
            .resolve(table.outputName)
            .resolve("expected")
            .resolve(s"${table.outputName}_$label")
            .resolve("expected_data")
          RowComparison.assertRowsEqual(
            spark.sql(oracle),
            spark.read.parquet(expectedPath.toString),
            s"${table.outputName}/$label literal oracle"
          )
        }
      }

      snapshotSpec(t).assert { spec =>
        spec.expectation match {
          case Succeeded(result) =>
            val schema = DataType.fromJson(result.metadata.schemaString).asInstanceOf[StructType]
            require(schema == StructType.fromDDL("id INT, value INT, p INT"))
            require(
              result.metadata.partitionColumns ==
              (if (partitioned) Seq("p") else Seq.empty)
            )
          case other => fail(s"${table.outputName} snapshot must succeed: $other")
        }
      }
    }
  }

  test("signed_zero") {
    setForTest("spark.sql.parquet.filterPushdown", "false")
    require(
      spark.conf.get("spark.sql.parquet.filterPushdown") == "false",
      "The signed-zero bag is validated with Parquet filter pushdown disabled"
    )
    sql("CREATE TABLE tbl (id INT, f FLOAT, d DOUBLE) USING delta")
    // Separate writes keep the two zero signs and nonzero ranges in distinct files.
    sql("""INSERT INTO tbl VALUES
      |(1, CAST('0.0' AS FLOAT), CAST('0.0' AS DOUBLE)),
      |(2, CAST('0.0' AS FLOAT), CAST('0.0' AS DOUBLE)),
      |(2, CAST('0.0' AS FLOAT), CAST('0.0' AS DOUBLE))""".stripMargin)
    sql("""INSERT INTO tbl VALUES
      |(3, CAST('-0.0' AS FLOAT), CAST('-0.0' AS DOUBLE)),
      |(4, CAST('-0.0' AS FLOAT), CAST('-0.0' AS DOUBLE)),
      |(4, CAST('-0.0' AS FLOAT), CAST('-0.0' AS DOUBLE))""".stripMargin)
    sql("INSERT INTO tbl VALUES (5, -1, -1), (6, -2, -2)")
    sql("INSERT INTO tbl VALUES (7, 1, 1), (8, 2, 2)")
    sql("INSERT INTO tbl VALUES (9, NULL, NULL)")
    val t = registerTable("tbl")
    val table = WorkloadContext.current.getTableSpec(t)

    // Literal bags do not filter the source table. IDs distinguish the original zero signs;
    // exact duplicates check multiplicity. Row equality need not retain a zero's sign bit.
    val zeros = Seq("(1, 0, 0)", "(2, 0, 0)", "(2, 0, 0)", "(3, 0, 0)", "(4, 0, 0)", "(4, 0, 0)")
    val negative = Seq("(5, -1, -1)", "(6, -2, -2)")
    val positive = Seq("(7, 1, 1)", "(8, 2, 2)")
    val nulls = Seq("(9, NULL, NULL)")
    val nonzero = negative ++ positive
    val all = zeros ++ nonzero ++ nulls
    val cases = Seq(("read_all", null, all)) ++
      Seq(("f", "FLOAT"), ("d", "DOUBLE")).flatMap {
        case (column, dataType) =>
          val plus = s"CAST('0.0' AS $dataType)"
          val minus = s"CAST('-0.0' AS $dataType)"
          val one = s"CAST(1 AS $dataType)"
          val negativeOne = s"CAST(-1 AS $dataType)"
          val typedNull = s"CAST(NULL AS $dataType)"
          val comparisons = for {
            (sign, zero) <- Seq(("positive", plus), ("negative", minus))
            (label, operator, bag) <- Seq(
              ("eq", "=", zeros),
              ("null_safe_eq", "<=>", zeros),
              ("ne", "!=", nonzero),
              ("lt", "<", negative),
              ("le", "<=", negative ++ zeros),
              ("gt", ">", positive),
              ("ge", ">=", zeros ++ positive)
            )
          } yield (s"${column}_${label}_$sign", s"$column $operator $zero", bag)
          val inLists = Seq(
            ("in_positive", plus, zeros),
            ("in_negative", minus, zeros),
            ("in_both", s"$plus, $minus", zeros),
            ("in_positive_null", s"$plus, $typedNull", zeros),
            ("in_negative_null", s"$minus, $typedNull", zeros),
            ("in_both_null", s"$plus, $minus, $typedNull", zeros),
            ("in_positive_nonzero", s"$plus, $one", zeros :+ "(7, 1, 1)"),
            ("in_negative_nonzero", s"$minus, $negativeOne", zeros :+ "(5, -1, -1)"),
            (
              "in_both_null_nonzero",
              s"$plus, $minus, $typedNull, $negativeOne, $one",
              zeros ++ Seq("(5, -1, -1)", "(7, 1, 1)")
            ),
            ("in_nonzero_null", s"$negativeOne, $one, $typedNull", Seq("(5, -1, -1)", "(7, 1, 1)")),
            ("in_null", typedNull, Seq.empty[String]),
            ("in_miss", s"CAST(9 AS $dataType), $typedNull", Seq.empty[String])
          )
          comparisons ++ Seq((s"${column}_null_safe_null", s"$column <=> $typedNull", nulls)) ++
          inLists.map {
            case (label, literals, bag) =>
              (s"${column}_$label", s"$column IN ($literals)", bag)
          }
      }

    for ((label, predicate, bag) <- cases) {
      readSpec(t, predicate = predicate, name = Some(label)).assert { spec =>
        require(spark.conf.get("spark.sql.parquet.filterPushdown") == "false")
        spec.expectation match {
          case Succeeded(result) => require(result.rowCount == bag.size)
          case other => fail(s"${table.outputName}/$label must succeed: $other")
        }
        val oracle = if (bag.nonEmpty) {
          "SELECT CAST(id AS INT) AS id, CAST(f AS FLOAT) AS f, CAST(d AS DOUBLE) AS d " +
          s"FROM VALUES ${bag.mkString(", ")} AS expected(id, f, d)"
        } else {
          "SELECT CAST(NULL AS INT) AS id, CAST(NULL AS FLOAT) AS f, " +
          "CAST(NULL AS DOUBLE) AS d WHERE false"
        }
        val expectedPath = outputDir
          .resolve(table.outputName)
          .resolve("expected")
          .resolve(s"${table.outputName}_$label")
          .resolve("expected_data")
        RowComparison.assertRowsEqual(
          spark.sql(oracle),
          spark.read.parquet(expectedPath.toString),
          s"${table.outputName}/$label literal oracle"
        )
      }
    }

    snapshotSpec(t).assert { spec =>
      spec.expectation match {
        case Succeeded(result) =>
          val schema = DataType.fromJson(result.metadata.schemaString).asInstanceOf[StructType]
          require(schema == StructType.fromDDL("id INT, f FLOAT, d DOUBLE"))
          require(result.metadata.partitionColumns.isEmpty)
        case other => fail(s"${table.outputName} snapshot must succeed: $other")
      }
    }
  }

  // Explicit predicate casts, precision boundaries and legacy narrowing.
  // Expected IDs are derived independently of the CAST predicates. Source duplicates stay intact.
  for ((subcase, sourceType, sourceRows, checks, legacy) <- Seq(
      (
        "float_double",
        "FLOAT",
        Seq(
          (1, "-0.1"),
          (2, "0.1"),
          (2, "0.1"),
          (3, "0.10000001"),
          (4, "0.2"),
          (5, "1.0"),
          (6, "NULL")
        ),
        Seq(
          ("read_all", null, Seq(1, 2, 3, 4, 5, 6)),
          ("read_hit", "CAST(value AS DOUBLE) = CAST('0.10000000149011612' AS DOUBLE)", Seq(2)),
          (
            "decimal_spelling_miss",
            "CAST(value AS DOUBLE) = CAST('0.1' AS DOUBLE)",
            Seq.empty[Int]
          ),
          ("above_decimal", "CAST(value AS DOUBLE) > CAST('0.1' AS DOUBLE)", Seq(2, 3, 4, 5)),
          (
            "adjacent_range",
            "CAST(value AS DOUBLE) >= CAST('0.10000000149011612' AS DOUBLE) AND " +
            "CAST(value AS DOUBLE) < CAST('0.10000000894069672' AS DOUBLE)",
            Seq(2)
          ),
          (
            "negative_hit",
            "CAST(value AS DOUBLE) = CAST('-0.10000000149011612' AS DOUBLE)",
            Seq(1)
          ),
          ("read_miss", "CAST(value AS DOUBLE) = CAST('2' AS DOUBLE)", Seq.empty[Int])
        ),
        false
      ),
      (
        "double_2p53",
        "BIGINT",
        Seq(
          (1, "9007199254740991"),
          (2, "9007199254740992"),
          (3, "9007199254740993"),
          (3, "9007199254740993"),
          (4, "9007199254740994"),
          (5, "9007199254740995"),
          (6, "-9007199254740993"),
          (7, "NULL")
        ),
        Seq(
          ("read_all", null, Seq(1, 2, 3, 4, 5, 6, 7)),
          ("read_hit", "CAST(value AS DOUBLE) = CAST('9007199254740992' AS DOUBLE)", Seq(2, 3)),
          ("adjacent_hit", "CAST(value AS DOUBLE) = CAST('9007199254740994' AS DOUBLE)", Seq(4)),
          (
            "adjacent_range",
            "CAST(value AS DOUBLE) > CAST('9007199254740992' AS DOUBLE) AND " +
            "CAST(value AS DOUBLE) < CAST('9007199254740996' AS DOUBLE)",
            Seq(4)
          ),
          (
            "below_boundary",
            "CAST(value AS DOUBLE) < CAST('9007199254740992' AS DOUBLE)",
            Seq(1, 6)
          ),
          (
            "read_miss",
            "CAST(value AS DOUBLE) = CAST('9007199254740998' AS DOUBLE)",
            Seq.empty[Int]
          )
        ),
        false
      ),
      (
        "float_2p24",
        "INT",
        Seq(
          (1, "16777215"),
          (2, "16777216"),
          (3, "16777217"),
          (3, "16777217"),
          (4, "16777218"),
          (5, "16777219"),
          (6, "-16777217"),
          (7, "NULL")
        ),
        Seq(
          ("read_all", null, Seq(1, 2, 3, 4, 5, 6, 7)),
          ("read_hit", "CAST(value AS FLOAT) = CAST('16777216' AS FLOAT)", Seq(2, 3)),
          ("adjacent_hit", "CAST(value AS FLOAT) = CAST('16777218' AS FLOAT)", Seq(4)),
          (
            "adjacent_range",
            "CAST(value AS FLOAT) > CAST('16777216' AS FLOAT) AND " +
            "CAST(value AS FLOAT) < CAST('16777220' AS FLOAT)",
            Seq(4)
          ),
          ("below_boundary", "CAST(value AS FLOAT) < CAST('16777216' AS FLOAT)", Seq(1, 6)),
          ("read_miss", "CAST(value AS FLOAT) = CAST('16777222' AS FLOAT)", Seq.empty[Int])
        ),
        false
      ),
      (
        "long_int_wrap",
        "BIGINT",
        Seq(
          (1, "4"),
          (2, "5"),
          (2, "5"),
          (3, "4294967301"),
          (3, "4294967301"),
          (4, "4294967300"),
          (5, "4294967302"),
          (6, "-4294967291"),
          (7, "NULL"),
          (8, "-1"),
          (9, "6")
        ),
        Seq(
          ("read_all", null, Seq(1, 2, 3, 4, 5, 6, 7, 8, 9)),
          ("read_hit", "CAST(value AS INT) = CAST('5' AS INT)", Seq(2, 3, 6)),
          ("lower_neighbor", "CAST(value AS INT) = CAST('4' AS INT)", Seq(1, 4)),
          ("upper_neighbor", "CAST(value AS INT) = CAST('6' AS INT)", Seq(5, 9)),
          ("below_five", "CAST(value AS INT) < CAST('5' AS INT)", Seq(1, 4, 8)),
          ("above_five", "CAST(value AS INT) > CAST('5' AS INT)", Seq(5, 9)),
          ("read_miss", "CAST(value AS INT) = CAST('7' AS INT)", Seq.empty[Int])
        ),
        true
      ),
      (
        "to_short_wrap",
        "INT",
        Seq(
          (1, "4"),
          (2, "5"),
          (2, "5"),
          (3, "65541"),
          (3, "65541"),
          (4, "65540"),
          (5, "65542"),
          (6, "-65531"),
          (7, "NULL"),
          (8, "-1"),
          (9, "6")
        ),
        Seq(
          ("read_all", null, Seq(1, 2, 3, 4, 5, 6, 7, 8, 9)),
          ("read_hit", "CAST(value AS SHORT) = CAST('5' AS SHORT)", Seq(2, 3, 6)),
          ("lower_neighbor", "CAST(value AS SHORT) = CAST('4' AS SHORT)", Seq(1, 4)),
          ("upper_neighbor", "CAST(value AS SHORT) = CAST('6' AS SHORT)", Seq(5, 9)),
          ("below_five", "CAST(value AS SHORT) < CAST('5' AS SHORT)", Seq(1, 4, 8)),
          ("above_five", "CAST(value AS SHORT) > CAST('5' AS SHORT)", Seq(5, 9)),
          ("read_miss", "CAST(value AS SHORT) = CAST('7' AS SHORT)", Seq.empty[Int])
        ),
        true
      ),
      (
        "int_byte_wrap",
        "INT",
        Seq(
          (1, "4"),
          (2, "5"),
          (2, "5"),
          (3, "261"),
          (3, "261"),
          (4, "260"),
          (5, "262"),
          (6, "-251"),
          (7, "NULL"),
          (8, "-1"),
          (9, "6")
        ),
        Seq(
          ("read_all", null, Seq(1, 2, 3, 4, 5, 6, 7, 8, 9)),
          ("read_hit", "CAST(value AS BYTE) = CAST('5' AS BYTE)", Seq(2, 3, 6)),
          ("lower_neighbor", "CAST(value AS BYTE) = CAST('4' AS BYTE)", Seq(1, 4)),
          ("upper_neighbor", "CAST(value AS BYTE) = CAST('6' AS BYTE)", Seq(5, 9)),
          ("below_five", "CAST(value AS BYTE) < CAST('5' AS BYTE)", Seq(1, 4, 8)),
          ("above_five", "CAST(value AS BYTE) > CAST('5' AS BYTE)", Seq(5, 9)),
          ("read_miss", "CAST(value AS BYTE) = CAST('7' AS BYTE)", Seq.empty[Int])
        ),
        true
      )
    )) {
    test(s"cast_filters_$subcase") {
      // ReadQuery has no session config field. Supply the same legacy session on every replay.
      if (legacy) {
        setForTest("spark.sql.ansi.enabled", "false")
        require(
          spark.conf.get("spark.sql.ansi.enabled") == "false",
          "Narrowing CAST fixtures require a non-ANSI session for capture and replay"
        )
      }
      sql(s"CREATE TABLE tbl (id INT, value $sourceType) USING delta")
      // Separate writes leave ordinary and boundary values in multiple stats-bearing files.
      for (batch <- sourceRows.grouped(2)) {
        val values = batch.map { case (id, value) => s"($id, $value)" }.mkString(", ")
        sql(s"INSERT INTO tbl VALUES $values")
      }
      val t = registerTable("tbl")
      val table = WorkloadContext.current.getTableSpec(t)
      for ((label, predicate, expectedIds) <- checks) {
        val bag = sourceRows.filter { case (id, _) => expectedIds.contains(id) }
        readSpec(t, predicate = predicate, name = Some(label)).assert { spec =>
          if (legacy) require(spark.conf.get("spark.sql.ansi.enabled") == "false")
          spec.expectation match {
            case Succeeded(result) => require(result.rowCount == bag.size)
            case other => fail(s"${table.outputName}/$label must succeed: $other")
          }
          // Only source-type casts construct these literal bags; no predicate CAST is reused.
          val oracle = if (bag.nonEmpty) {
            val values = bag.map { case (id, value) => s"($id, $value)" }.mkString(", ")
            s"SELECT CAST(id AS INT) AS id, CAST(value AS $sourceType) AS value " +
            s"FROM VALUES $values AS expected(id, value)"
          } else {
            s"SELECT CAST(NULL AS INT) AS id, CAST(NULL AS $sourceType) AS value WHERE false"
          }
          val expectedPath = outputDir
            .resolve(table.outputName)
            .resolve("expected")
            .resolve(s"${table.outputName}_$label")
            .resolve("expected_data")
          RowComparison.assertRowsEqual(
            spark.sql(oracle),
            spark.read.parquet(expectedPath.toString),
            s"${table.outputName}/$label literal oracle"
          )
        }
      }
      snapshotSpec(t).assert { spec =>
        spec.expectation match {
          case Succeeded(result) =>
            val schema = DataType.fromJson(result.metadata.schemaString).asInstanceOf[StructType]
            require(schema == StructType.fromDDL(s"id INT, value $sourceType"))
            require(result.metadata.partitionColumns.isEmpty)
            require(result.metadata.configuration.isEmpty)
            require(result.protocol.minReaderVersion == 1)
            require(result.protocol.minWriterVersion == 2)
          case other => fail(s"${table.outputName} snapshot must succeed: $other")
        }
      }
      if (subcase == "float_double") {
        val compositionCases = Seq(
          ("direct_nonnegative", "CAST(value AS DOUBLE) >= 0", Seq(2, 3, 4, 5)),
          (
            "compound_hit",
            "(CAST(value AS DOUBLE) > 0.15 AND id < 5) OR " +
              "(CAST(value AS DOUBLE) < 0 AND id = 1)",
            Seq(1, 4)
          ),
          (
            "compound_miss",
            "(CAST(value AS DOUBLE) > 2 AND id < 5) OR " +
              "(CAST(value AS DOUBLE) < -2 AND id = 1)",
            Seq.empty[Int]
          )
        )
        for ((label, predicate, ids) <- compositionCases) {
          val bag = sourceRows.filter { case (id, _) => ids.contains(id) }
          readSpec(t, predicate = predicate, name = Some(label)).assert { spec =>
            spec.expectation match {
              case Succeeded(result) => require(result.rowCount == bag.size)
              case other => fail(s"${table.outputName}/$label must succeed: $other")
            }
            val oracle = if (bag.nonEmpty) {
              val values = bag.map { case (id, value) => s"($id, $value)" }.mkString(", ")
              s"SELECT CAST(id AS INT) AS id, CAST(value AS FLOAT) AS value " +
                s"FROM VALUES $values AS expected(id, value)"
            } else {
              "SELECT CAST(NULL AS INT) AS id, CAST(NULL AS FLOAT) AS value WHERE false"
            }
            val expectedPath = outputDir
              .resolve(table.outputName)
              .resolve("expected")
              .resolve(s"${table.outputName}_$label")
              .resolve("expected_data")
            RowComparison.assertRowsEqual(
              spark.sql(oracle),
              spark.read.parquet(expectedPath.toString),
              s"${table.outputName}/$label literal oracle"
            )
          }
        }
      }

    }
  }

  // Complex filter row semantics. File counts do not prove filter pushdown.
  test("complex_filters_month_timestamp") {
    // Read specs have no timezone field; use UTC through queued capture and every replay.
    require(spark.conf.get("spark.sql.session.timeZone") == "UTC")
    sql("CREATE TABLE tbl (id INT, ts TIMESTAMP) USING delta")
    val rows = Seq(
      "(1, TIMESTAMP'2023-12-31 23:59:59.999999')",
      "(2, TIMESTAMP'2024-01-01 00:00:00')",
      "(3, TIMESTAMP'2024-01-31 23:59:59.999999')",
      "(4, TIMESTAMP'2024-02-01 00:00:00')",
      "(5, TIMESTAMP'2024-02-29 23:59:59.999999')",
      "(6, TIMESTAMP'2024-03-01 00:00:00')",
      "(7, TIMESTAMP'2024-12-31 23:59:59.999999')",
      "(8, TIMESTAMP'2025-01-01 00:00:00')",
      "(8, TIMESTAMP'2025-01-01 00:00:00')",
      "(9, NULL)"
    )
    for (batch <- rows.grouped(2)) {
      sql(s"INSERT INTO tbl VALUES ${batch.mkString(", ")}")
    }
    val t = registerTable("tbl")
    val table = WorkloadContext.current.getTableSpec(t)
    // Enumerated source IDs preserve duplicates without evaluating the predicate in the oracle.
    val cases = Seq(
      ("read_all", null, Seq(1, 2, 3, 4, 5, 6, 7, 8, 9)),
      ("january", "month(ts) = 1", Seq(2, 3, 8)),
      ("february", "month(ts) = 2", Seq(4, 5)),
      ("december", "month(ts) = 12", Seq(1, 7)),
      ("miss_june", "month(ts) = 6", Seq.empty[Int]),
      ("null_month", "month(ts) IS NULL", Seq(9))
    )
    for ((label, predicate, ids) <- cases) {
      val bag = rows.filter(row => ids.contains(row.drop(1).takeWhile(_ != ',').toInt))
      readSpec(t, predicate = predicate, name = Some(label)).assert { spec =>
        spec.expectation match {
          case Succeeded(result) => require(result.rowCount == bag.size)
          case other => fail(s"${table.outputName}/$label must succeed: $other")
        }
        val values = if (bag.nonEmpty) bag.mkString(", ") else "(NULL, NULL)"
        val projection = "CAST(id AS INT) AS id, CAST(ts AS TIMESTAMP) AS ts"
        val oracle = s"SELECT $projection FROM VALUES $values AS expected(id, ts)" +
          (if (bag.nonEmpty) "" else " WHERE false")
        val expectedPath = outputDir
          .resolve(table.outputName)
          .resolve("expected")
          .resolve(s"${table.outputName}_$label")
          .resolve("expected_data")
        RowComparison.assertRowsEqual(
          spark.sql(oracle),
          spark.read.parquet(expectedPath.toString),
          s"${table.outputName}/$label literal oracle"
        )
      }
    }
    snapshotSpec(t).assert { spec =>
      spec.expectation match {
        case Succeeded(result) =>
          val schema = DataType.fromJson(result.metadata.schemaString).asInstanceOf[StructType]
          require(schema == StructType.fromDDL("id INT, ts TIMESTAMP"))
          require(result.metadata.partitionColumns.isEmpty)
          require(result.metadata.configuration.isEmpty)
          require(result.protocol.minReaderVersion == 1)
          require(result.protocol.minWriterVersion == 2)
        case other => fail(s"${table.outputName} snapshot must succeed: $other")
      }
    }
  }

  test("complex_filters_strings") {
    sql("CREATE TABLE tbl (id INT, name STRING, s STRUCT<name: STRING>) USING delta")
    val rows = Seq(
      "(1, 'apple', named_struct('name', 'apple'))",
      "(2, 'banana', named_struct('name', 'banana'))",
      "(3, 'alice', named_struct('name', 'alice'))",
      "(4, 'bob', named_struct('name', 'bob'))",
      "(5, 'charlie', named_struct('name', 'charlie'))",
      "(6, 'diana', named_struct('name', 'diana'))",
      "(7, 'Apple', named_struct('name', 'APPLE'))",
      "(8, 'ALICE', named_struct('name', 'Alice'))",
      "(8, 'ALICE', named_struct('name', 'Alice'))",
      "(9, '', named_struct('name', ''))",
      "(10, NULL, named_struct('name', NULL))",
      "(11, 'apPle', named_struct('name', NULL))"
    )
    for (batch <- rows.grouped(2)) {
      sql(s"INSERT INTO tbl VALUES ${batch.mkString(", ")}")
    }
    val t = registerTable("tbl")
    val table = WorkloadContext.current.getTableSpec(t)
    // Enumerated source IDs preserve duplicates without evaluating the predicate in the oracle.
    val cases = Seq(
      ("read_all", null, Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)),
      ("prefix_hit", "startswith(name, 'app')", Seq(1)),
      ("prefix_a", "startswith(name, 'a')", Seq(1, 3, 11)),
      ("prefix_nested", "startswith(s.name, 'a')", Seq(1, 3)),
      ("prefix_miss", "startswith(name, 'z')", Seq.empty[Int]),
      ("prefix_empty", "startswith(name, '')", Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 11)),
      ("prefix_null", "startswith(name, CAST(NULL AS STRING))", Seq.empty[Int]),
      ("prefix_null_result", "startswith(name, 'a') IS NULL", Seq(10)),
      ("upper_alice", "upper(name) = 'ALICE'", Seq(3, 8)),
      ("upper_apple", "upper(name) = 'APPLE'", Seq(1, 7, 11)),
      ("upper_miss", "upper(name) = 'ZEBRA'", Seq.empty[Int]),
      ("upper_null", "upper(name) IS NULL", Seq(10)),
      ("upper_selective", "upper(name) = 'APPLE' AND id > 1", Seq(7, 11))
    )
    for ((label, predicate, ids) <- cases) {
      val bag = rows.filter(row => ids.contains(row.drop(1).takeWhile(_ != ',').toInt))
      readSpec(t, predicate = predicate, name = Some(label)).assert { spec =>
        spec.expectation match {
          case Succeeded(result) => require(result.rowCount == bag.size)
          case other => fail(s"${table.outputName}/$label must succeed: $other")
        }
        val values = if (bag.nonEmpty) bag.mkString(", ") else "(NULL, NULL, NULL)"
        val projection = "CAST(id AS INT) AS id, CAST(name AS STRING) AS name, " +
          "CAST(s AS STRUCT<name: STRING>) AS s"
        val oracle = s"SELECT $projection FROM VALUES $values AS expected(id, name, s)" +
          (if (bag.nonEmpty) "" else " WHERE false")
        val expectedPath = outputDir
          .resolve(table.outputName)
          .resolve("expected")
          .resolve(s"${table.outputName}_$label")
          .resolve("expected_data")
        RowComparison.assertRowsEqual(
          spark.sql(oracle),
          spark.read.parquet(expectedPath.toString),
          s"${table.outputName}/$label literal oracle"
        )
      }
    }
    snapshotSpec(t).assert { spec =>
      spec.expectation match {
        case Succeeded(result) =>
          val schema = DataType.fromJson(result.metadata.schemaString).asInstanceOf[StructType]
          require(schema == StructType.fromDDL("id INT, name STRING, s STRUCT<name: STRING>"))
          require(result.metadata.partitionColumns.isEmpty)
          require(result.metadata.configuration.isEmpty)
          require(result.protocol.minReaderVersion == 1)
          require(result.protocol.minWriterVersion == 2)
        case other => fail(s"${table.outputName} snapshot must succeed: $other")
      }
    }
  }

  test("complex_filters_deeply_nested") {
    sql(
      "CREATE TABLE tbl (id INT, s STRUCT<a: INT, b: INT, c: INT, d: INT, e: INT, " +
      "f: INT, g: INT, h: INT, i: INT, j: INT>) USING delta"
    )
    val rows = Seq(
      "(1, named_struct('a', 1, 'b', 1, 'c', NULL, 'd', NULL, 'e', NULL, 'f', NULL, " +
      "'g', NULL, 'h', NULL, 'i', NULL, 'j', NULL))",
      "(2, named_struct('a', 1, 'b', 0, 'c', 0, 'd', 1, 'e', NULL, 'f', NULL, 'g', " +
      "NULL, 'h', NULL, 'i', NULL, 'j', NULL))",
      "(3, named_struct('a', 1, 'b', 0, 'c', 0, 'd', 0, 'e', 1, 'f', 1, 'g', NULL, " +
      "'h', NULL, 'i', NULL, 'j', NULL))",
      "(4, named_struct('a', 1, 'b', 0, 'c', 0, 'd', 0, 'e', 1, 'f', 0, 'g', 0, 'h', " +
      "1, 'i', NULL, 'j', NULL))",
      "(5, named_struct('a', 1, 'b', 0, 'c', 0, 'd', 0, 'e', 1, 'f', 0, 'g', 0, 'h', " +
      "0, 'i', 1, 'j', 0))",
      "(5, named_struct('a', 1, 'b', 0, 'c', 0, 'd', 0, 'e', 1, 'f', 0, 'g', 0, 'h', " +
      "0, 'i', 1, 'j', 0))",
      "(6, named_struct('a', 0, 'b', 1, 'c', 0, 'd', 1, 'e', 1, 'f', 1, 'g', 0, 'h', " +
      "1, 'i', 1, 'j', 0))",
      "(7, named_struct('a', 1, 'b', 0, 'c', 1, 'd', 1, 'e', 1, 'f', 1, 'g', 0, 'h', " +
      "1, 'i', 1, 'j', 0))",
      "(8, named_struct('a', 1, 'b', 0, 'c', 0, 'd', 0, 'e', 1, 'f', 0, 'g', 0, 'h', " +
      "0, 'i', 1, 'j', 1))",
      "(9, named_struct('a', 1, 'b', 0, 'c', 0, 'd', 0, 'e', 1, 'f', 0, 'g', 0, 'h', " +
      "0, 'i', 1, 'j', NULL))",
      "(10, named_struct('a', NULL, 'b', 1, 'c', 0, 'd', 1, 'e', 1, 'f', 1, 'g', 0, " +
      "'h', 1, 'i', 1, 'j', 0))",
      "(11, named_struct('a', NULL, 'b', NULL, 'c', NULL, 'd', NULL, 'e', NULL, 'f', " +
      "NULL, 'g', NULL, 'h', NULL, 'i', NULL, 'j', NULL))",
      "(12, named_struct('a', 1, 'b', 0, 'c', 0, 'd', 0, 'e', 0, 'f', 1, 'g', 0, 'h', " +
      "1, 'i', 1, 'j', 0))"
    )
    for (batch <- rows.grouped(2)) {
      sql(s"INSERT INTO tbl VALUES ${batch.mkString(", ")}")
    }
    val t = registerTable("tbl")
    val table = WorkloadContext.current.getTableSpec(t)
    // Enumerated source IDs preserve duplicates without evaluating the predicate in the oracle.
    val cases = Seq(
      ("read_all", null, Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)),
      (
        "deep_hit",
        "s.a = 1 AND (s.b = 1 OR (NOT (s.c = 1) AND (s.d = 1 OR (s.e = 1 AND (s.f = " +
        "1 OR (NOT (s.g = 1) AND (s.h = 1 OR (s.i = 1 AND NOT (s.j = 1)))))))))",
        Seq(1, 2, 3, 4, 5)
      ),
      (
        "deep_false",
        "NOT (s.a = 1 AND (s.b = 1 OR (NOT (s.c = 1) AND (s.d = 1 OR (s.e = 1 AND " +
        "(s.f = 1 OR (NOT (s.g = 1) AND (s.h = 1 OR (s.i = 1 AND NOT (s.j = 1))))))))))",
        Seq(6, 7, 8, 12)
      ),
      (
        "deep_null",
        "(s.a = 1 AND (s.b = 1 OR (NOT (s.c = 1) AND (s.d = 1 OR (s.e = 1 AND (s.f = " +
        "1 OR (NOT (s.g = 1) AND (s.h = 1 OR (s.i = 1 AND NOT (s.j = 1)))))))))) IS " +
        "NULL",
        Seq(9, 10, 11)
      ),
      (
        "deep_miss",
        "(s.a = 1 AND (s.b = 1 OR (NOT (s.c = 1) AND (s.d = 1 OR (s.e = 1 AND (s.f = " +
        "1 OR (NOT (s.g = 1) AND (s.h = 1 OR (s.i = 1 AND NOT (s.j = 1)))))))))) AND " +
        "id = 99",
        Seq.empty[Int]
      )
    )
    for ((label, predicate, ids) <- cases) {
      val bag = rows.filter(row => ids.contains(row.drop(1).takeWhile(_ != ',').toInt))
      readSpec(t, predicate = predicate, name = Some(label)).assert { spec =>
        spec.expectation match {
          case Succeeded(result) => require(result.rowCount == bag.size)
          case other => fail(s"${table.outputName}/$label must succeed: $other")
        }
        val values = if (bag.nonEmpty) bag.mkString(", ") else "(NULL, NULL)"
        val projection = "CAST(id AS INT) AS id, CAST(s AS STRUCT<a: INT, b: INT, c: INT, " +
          "d: INT, e: INT, f: INT, g: INT, h: INT, i: INT, j: INT>) AS s"
        val oracle = s"SELECT $projection FROM VALUES $values AS expected(id, s)" +
          (if (bag.nonEmpty) "" else " WHERE false")
        val expectedPath = outputDir
          .resolve(table.outputName)
          .resolve("expected")
          .resolve(s"${table.outputName}_$label")
          .resolve("expected_data")
        RowComparison.assertRowsEqual(
          spark.sql(oracle),
          spark.read.parquet(expectedPath.toString),
          s"${table.outputName}/$label literal oracle"
        )
      }
    }
    snapshotSpec(t).assert { spec =>
      spec.expectation match {
        case Succeeded(result) =>
          val schema = DataType.fromJson(result.metadata.schemaString).asInstanceOf[StructType]
          require(
            schema == StructType.fromDDL(
              "id INT, s STRUCT<a: INT, b: INT, c: INT, d: INT, e: INT, f: INT, " +
              "g: INT, h: INT, i: INT, j: INT>"
            )
          )
          require(result.metadata.partitionColumns.isEmpty)
          require(result.metadata.configuration.isEmpty)
          require(result.protocol.minReaderVersion == 1)
          require(result.protocol.minWriterVersion == 2)
        case other => fail(s"${table.outputName} snapshot must succeed: $other")
      }
    }
  }

  // Incomplete or selected file statistics.
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

  // Timestamp statistics: encodings are checked on disk, independently of the table
  // reader.
  private val microFirst = "2024-01-01T00:00:00.123456Z"
  private val microLast = "2024-01-01T00:00:00.123789Z"
  private val nanoLower = "2024-01-01T00:00:00.123455111Z"
  private val nanoUpper = "2024-01-01T00:00:00.123789999Z"
  private val microSchema = new StructType().add("id", IntegerType).add("ts", TimestampType)

  // Literal instants, not values read back from the table, are the row oracle.
  private def microRows: Seq[Row] = Seq(
    Row(1, Timestamp.from(Instant.parse(microFirst))),
    Row(2, Timestamp.from(Instant.parse(microLast))),
    Row(2, Timestamp.from(Instant.parse(microLast))))

  private def createMicroTable(): TableHandle = {
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

  private def checkMicroSpecs(t: TableHandle, checkJsonStats: Boolean = false): Unit = {
    val workloadName = WorkloadContext.current.workloadName
    val tableOutputDir = outputDir.resolve(workloadName)
    val queries = Seq(
      ("all", null, microRows),
      ("lower_edge", "ts <= TIMESTAMP '2024-01-01 00:00:00.123456'", microRows.take(1)),
      ("upper_edge", "ts >= TIMESTAMP '2024-01-01 00:00:00.123789'", microRows.drop(1)),
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
          val expected = spark.createDataFrame(rows.asJava, microSchema)
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
            require(DataType.fromJson(result.metadata.schemaString) == microSchema)
          case other => fail(s"$suffix snapshot must succeed: $other")
        }
      }
    }
  }

  test("nanosecond_stats_json") {
    val t = createMicroTable()
    // The bounds enclose the microsecond rows. All of digits 7-9 are nonzero.
    val lower = nanoLower
    val upper = nanoUpper
    require(Instant.parse(lower).isBefore(Instant.parse(microFirst)))
    require(Instant.parse(upper).isAfter(Instant.parse(microLast)))
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
    checkMicroSpecs(t, checkJsonStats = true)
  }

  for ((caseName, encoding, lower, upper) <- Seq(
      ("millisecond_stats_checkpoint", "TIMESTAMP_MILLIS",
        "2024-01-01T00:00:00.123Z", "2024-01-01T00:00:00.124Z"),
      ("microsecond_stats_checkpoint", "TIMESTAMP_MICROS", microFirst, microLast),
      ("int96_stats_checkpoint", "INT96", microFirst, microLast))) {
    test(caseName) {
      val t = createMicroTable()
      require(!Instant.parse(lower).isAfter(Instant.parse(microFirst)))
      require(!Instant.parse(upper).isBefore(Instant.parse(microLast)))
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
        checkMicroCheckpoint(checkpoint, encoding, lower, upper)
        if (encoding == "INT96") {
          // Spark's writer only emits microsecond-aligned INT96. Preserve every checkpoint
          // field with the existing Parquet Group API, replacing just the two stats cells.
          rewriteInt96Bounds(checkpoint)
          checkMicroCheckpoint(checkpoint, encoding, nanoLower, nanoUpper)
        }
        // Retain v1 JSON as a discovery/metadata tail alongside the checkpoint. Removing v0
        // prevents full JSON replay, so v1 and latest require checkpoint bootstrap.
        val files = Files.list(logDir)
        try files.iterator().asScala.filter { p =>
          p.getFileName.toString == "00000000000000000000.json" ||
            p.toString.endsWith(".crc")
        }.foreach(Files.delete(_)) finally files.close()
      }
      checkMicroSpecs(t)
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

  private def checkMicroCheckpoint(
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
    setForTest("spark.sql.session.timeZone", writerZone)
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
    setForTest("spark.sql.session.timeZone", readerZone)
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

  private def pruneHistoryExcept(dir: Path, version: Long): Unit = {
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
      setForTest("spark.sql.session.timeZone", "UTC")
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
        pruneHistoryExcept(dir, 1L)
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
      checkLiteralRead(table, "all", expectedSchema, rows)
      checkLiteralRead(table, "all_v1", expectedSchema, rows, version = 1L)
      checkLiteralRead(table, "hit", expectedSchema, rows, predicate = s"v = $literal")
      checkLiteralRead(table, "miss", expectedSchema, Seq.empty,
        predicate = s"v < $literal OR v > $literal")
      checkLiteralSnapshot(table, expectedSchema)
      checkLiteralSnapshot(table, expectedSchema, version = 1L)
    }
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
      setForTest("spark.sql.session.timeZone", "UTC")
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
        removeVersionChecksums(dir)
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
      checkLiteralRead(table, "all", expectedSchema, rows)
      checkLiteralRead(table, "all_v1", expectedSchema, rows, version = 1L)
      checkLiteralRead(table, "hit", expectedSchema, rows, predicate = s"v = $literal")
      checkLiteralRead(table, "miss", expectedSchema, Seq.empty,
        predicate = s"v < $literal OR v > $literal")
      checkLiteralSnapshot(table, expectedSchema)
      checkLiteralSnapshot(table, expectedSchema, version = 1L)
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
      setForTest("spark.sql.session.timeZone", "UTC")
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
          pruneHistoryExcept(dir, 1L)
        } else {
          removeVersionChecksums(dir)
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

      checkLiteralRead(table, "all", intMaxSchema, rows)
      checkLiteralRead(table, "all_v1", intMaxSchema, rows, version = 1L)
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
          checkLiteralRead(table, s"${field}_$label", intMaxSchema,
            rows.filter(r => index(r).exists(selected)), predicate)
        }
        checkLiteralRead(table, s"${field}_is_null", intMaxSchema,
          rows.filter(_.getInt(0) == 7), s"$field IS NULL")
      }
      checkLiteralSnapshot(table, intMaxSchema, Seq("p_ts", "p_ntz"))
      checkLiteralSnapshot(table, intMaxSchema, Seq("p_ts", "p_ntz"), version = 1L)
    }
  }

  // DATE32 epoch-day bounds. 2147483648 is outside DATE32 and is not an input. Rows use
  // java.time.LocalDate: java.sql.Date misrenders these years. Raw INT32 days are the oracle.
  test("date_epoch_days_int32_bounds") {
    setForTest("spark.sql.session.timeZone", "UTC")
    setForTest("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    setForTest("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    // Spark's data-filter translation rebases java.sql.Date literals to Julian days, which
    // overflows near -2147483648. LocalDate literals avoid that reference-side conversion.
    // setForTest keeps it through capture and restores it after the test, including on failure.
    setForTest(java8Key, "true")
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
      removeVersionChecksums(dir)
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
    checkLiteralRead(table, "all", expectedSchema, rows)
    checkLiteralRead(table, "all_v1", expectedSchema, rows, version = 1L)
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
        checkLiteralRead(table, s"${field}_$label", expectedSchema,
          rows.filter(r => index(r).exists(selected)), predicate)
      }
      checkLiteralRead(table, s"${field}_is_null", expectedSchema,
        rows.filter(_.getInt(0) == 6), s"$field IS NULL")
    }
    checkLiteralSnapshot(table, expectedSchema, Seq("p"))
    checkLiteralSnapshot(table, expectedSchema, Seq("p"), version = 1L)
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

  // Table operations that rewrite statistics or file layout.
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

}
