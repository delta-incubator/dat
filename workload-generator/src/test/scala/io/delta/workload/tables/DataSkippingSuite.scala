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

import io.delta.workload.{JsonUtil, WorkloadTestSuite}
import io.delta.workload.log.AddFile

/**
 * Data skipping, statistics, and partitioning workloads.
 * Covers: equality, range, IN, IS NULL, BETWEEN, LIKE, NOT, AND/OR combinations,
 * nested field predicates, boolean predicates, typed stats, null handling,
 * multiple files for skipping, missing stats, long strings, column mapping stats,
 * generated columns, DVs, partitioned skipping, partition pruning, projection,
 * stats edge cases (null min/max, numRecords-only, truncated strings).
 */
class DataSkippingSuite extends WorkloadTestSuite("data_skipping") {

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
    readSpec(t, predicate = "NOT (a <=> 2)", name = "read_not_a_nse_2")
    readSpec(t, predicate = "true", name = "read_true")
    // misses
    readSpec(t, predicate = "NOT (a = 1)", name = "read_miss_not_a_eq_1")
    readSpec(t, predicate = "NOT (a <=> 1)", name = "read_miss_not_a_nse_1")
    readSpec(t, predicate = "a = 2", name = "read_miss_a_eq_2")
    readSpec(t, predicate = "a <=> 2", name = "read_miss_a_nse_2")
    readSpec(t, predicate = "a > 1", name = "read_miss_a_gt_1")
    readSpec(t, predicate = "a >= 2", name = "read_miss_a_gte_2")
    readSpec(t, predicate = "a <= 0", name = "read_miss_a_lte_0")
    readSpec(t, predicate = "a = 0", name = "read_miss_a_eq_0")
    readSpec(t, predicate = "a > 2", name = "read_miss_a_gt_2")
    readSpec(t, predicate = "a < 1", name = "read_miss_a_lt_1")
    readSpec(t, predicate = "a <> 1", name = "read_miss_a_neq_1")
    readSpec(t, predicate = "1 != a", name = "read_miss_1_neq_a")
    readSpec(t, predicate = "2 <=> a", name = "read_miss_2_nse_a")
    readSpec(t, predicate = "0 >= a", name = "read_miss_0_gte_a")
    readSpec(t, predicate = "0 = a", name = "read_miss_0_eq_a")
    readSpec(t, predicate = "1 > a", name = "read_miss_1_gt_a")
    readSpec(t, predicate = "1 < a", name = "read_miss_1_lt_a")
    readSpec(t, predicate = "0 > a", name = "read_miss_0_gt_a")
    readSpec(t, predicate = "2 = a", name = "read_miss_2_eq_a")
    readSpec(t, predicate = "2 <= a", name = "read_miss_2_lte_a")
    readSpec(t, predicate = "0 < a AND a < 1", name = "read_miss_between_0_1")
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
    readSpec(t, predicate = "a.b = 2", name = "read_miss_ab_eq_2")
    readSpec(t, predicate = "a.b > 1", name = "read_miss_ab_gt_1")
    readSpec(t, predicate = "a.b < 1", name = "read_miss_ab_lt_1")
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
    readSpec(t, predicate = "a.b.c = 2", name = "read_miss_abc_eq_2")
    readSpec(t, predicate = "a.b.c > 1", name = "read_miss_abc_gt_1")
    readSpec(t, predicate = "a.b.c < 1", name = "read_miss_abc_lt_1")
    snapshotSpec(t)
  }

  test("nested_struct_predicate") {
    sql("CREATE TABLE tbl (id INT, info STRUCT<score: INT, name: STRING>) USING delta")
    sql("INSERT INTO tbl VALUES (1, named_struct('score', 90, 'name', 'alice'))")
    sql("INSERT INTO tbl VALUES (2, named_struct('score', 50, 'name', 'bob'))")
    val t = registerTable("tbl")
    readSpec(t, predicate = "info.score > 80")
    readSpec(t, predicate = "info.score < 40", name = "read_miss_low_score")
    snapshotSpec(t)
  }

  test("complex_nested") {
    sql("CREATE TABLE tbl (a INT, b STRUCT<x: INT, y: INT>) USING delta")
    sql("INSERT INTO tbl VALUES (1, named_struct('x', 10, 'y', 20))")
    sql("INSERT INTO tbl VALUES (2, named_struct('x', 30, 'y', 40))")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 1 AND b.x = 10")
    readSpec(t, predicate = "b.x > 20 OR b.y < 25")
    readSpec(t, predicate = "a > 5 AND b.x > 50", name = "read_miss_complex")
    snapshotSpec(t)
  }

  test("and_simple") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a <= 1 AND a > -1", name = "read_hit_and_bound")
    readSpec(t, predicate = "a >= 1 AND a <= 2", name = "read_hit_and_range")
    readSpec(t, predicate = "a > 5 AND a < 10", name = "read_miss_and_outside")
    snapshotSpec(t)
  }

  test("and_two_fields") {
    sql("CREATE TABLE tbl (a LONG, b LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1, 10), (2, 20)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 1 AND b = 10")
    readSpec(t, predicate = "a >= 1 AND b <= 20")
    readSpec(t, predicate = "a = 1 AND b > 100", name = "read_miss_b_out")
    readSpec(t, predicate = "a > 5 AND b > 5", name = "read_miss_both_out")
    snapshotSpec(t)
  }

  test("and_one_side_unsupported") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 1 AND CAST(a AS STRING) LIKE '%1'")
    readSpec(t, predicate = "a > 5 AND CAST(a AS STRING) LIKE '%x'", name = "read_miss_and_unsupported")
    snapshotSpec(t)
  }

  test("or_simple") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 1 OR a = 3")
    readSpec(t, predicate = "a < 0 OR a > 0")
    readSpec(t, predicate = "a = 5 OR a = 6", name = "read_miss_or")
    snapshotSpec(t)
  }

  test("or_two_fields") {
    sql("CREATE TABLE tbl (a LONG, b LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1, 10), (2, 20)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a = 1 OR b = 20")
    readSpec(t, predicate = "a = 5 OR b = 10")
    readSpec(t, predicate = "a > 0 OR b > 0")
    readSpec(t, predicate = "a = 5 OR b = 50", name = "read_miss_or_both")
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
    readSpec(t, predicate = "NOT (a < 0)", name = "read_not_lt_0")
    snapshotSpec(t)
  }

  test("not_and") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "NOT (a > 5 AND a < 10)")
    readSpec(t, predicate = "NOT (a > 0 AND a < 3)")
    readSpec(t, predicate = "NOT (a = 1 AND a = 2)", name = "read_not_and_contra")
    snapshotSpec(t)
  }

  test("not_or") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "NOT (a > 5 OR a < -5)")
    readSpec(t, predicate = "NOT (a = 1 OR a = 2)", name = "read_not_or_all")
    snapshotSpec(t)
  }

  test("starts_with") {
    sql("CREATE TABLE tbl (a STRING) USING delta")
    sql("INSERT INTO tbl VALUES ('apple'), ('banana')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a LIKE 'a%'")
    readSpec(t, predicate = "a LIKE 'b%'")
    readSpec(t, predicate = "a LIKE 'app%'")
    readSpec(t, predicate = "a LIKE 'z%'", name = "read_miss_z")
    readSpec(t, predicate = "a LIKE 'c%'", name = "read_miss_c")
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
    readSpec(t, predicate = "a.b LIKE 'z%'", name = "read_miss_z")
    readSpec(t, predicate = "a.b LIKE 'c%'", name = "read_miss_c")
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
    readSpec(t, predicate = "name LIKE 'z%'", name = "read_miss_z")
    readSpec(t, predicate = "name > 'e'", name = "read_miss_gt_e")
    readSpec(t, predicate = "name LIKE 'x%'", name = "read_miss_x")
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
    readSpec(t, predicate = "a = 'z'", name = "read_miss_z")
    readSpec(t, predicate = "a < 'a'", name = "read_miss_lt_a")
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
    readSpec(t, predicate = "a = 'a'", name = "read_miss_a")
    readSpec(t, predicate = "a < 'z'", name = "read_miss_lt_z")
    readSpec(t, predicate = "a > 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'", name = "read_miss_gt_long_z")
    snapshotSpec(t)
  }

  test("in_set") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a IN (1, 2)")
    readSpec(t, predicate = "a IN (3)")
    readSpec(t, predicate = "a IN (10, 20)", name = "read_miss_in")
    snapshotSpec(t)
  }

  test("in_list") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (10), (20)")
    sql("INSERT INTO tbl VALUES (30), (40)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a IN (10, 30)")
    readSpec(t, predicate = "a IN (99)", name = "read_miss_in_99")
    snapshotSpec(t)
  }

  test("in_nested") {
    sql("CREATE TABLE tbl (s STRUCT<x: INT>) USING delta")
    sql("INSERT INTO tbl VALUES (named_struct('x', 1))")
    sql("INSERT INTO tbl VALUES (named_struct('x', 5))")
    val t = registerTable("tbl")
    readSpec(t, predicate = "s.x IN (1, 5)")
    readSpec(t, predicate = "s.x IN (99)", name = "read_miss_nested_in")
    snapshotSpec(t)
  }

  test("in_with_nulls_mixed") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a IN (1, NULL)")
    readSpec(t, predicate = "a IN (99, NULL)", name = "read_in_null_miss")
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
      name = "read_in_large")
    readSpec(t, predicate = "a IN (100, 200)", name = "read_miss_in_large")
    snapshotSpec(t)
  }

  test("not_in") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a NOT IN (4, 5)")
    readSpec(t, predicate = "a NOT IN (1, 2, 3)", name = "read_not_in_all")
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
    readSpec(t, predicate = "a IS NOT NULL", name = "read_is_not_null")
    readSpec(t, predicate = "a = 1", name = "read_eq_1")
    readSpec(t, predicate = "a > 0", name = "read_gt_0")
    readSpec(t, predicate = "a < 0", name = "read_lt_0")
    readSpec(t, predicate = "a >= 0", name = "read_gte_0")
    readSpec(t, predicate = "a <= 0", name = "read_lte_0")
    readSpec(t, predicate = "a <=> NULL", name = "read_nse_null")
    readSpec(t, predicate = "a <=> 1", name = "read_nse_1")
    readSpec(t, predicate = "a IN (1, 2)", name = "read_in_1_2")
    readSpec(t, predicate = "NOT (a = 1)", name = "read_not_eq_1")
    readSpec(t, predicate = "NOT (a IS NULL)", name = "read_not_is_null")
    readSpec(t, predicate = "NOT (a IS NOT NULL)", name = "read_not_is_not_null")
    readSpec(t, predicate = "a = 1 OR a IS NULL", name = "read_eq_or_null")
    readSpec(t, predicate = "a = 1 AND a IS NULL", name = "read_eq_and_null")
    readSpec(t, predicate = "a LIKE 'x%'", name = "read_like_x")
    snapshotSpec(t)
  }

  test("nulls_only_nonnull") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a IS NULL", name = "read_is_null")
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
    readSpec(t, predicate = "a <=> NULL", name = "read_nse_null")
    readSpec(t, predicate = "a <=> 1", name = "read_nse_1")
    readSpec(t, predicate = "a IN (1, 3)")
    readSpec(t, predicate = "a IN (5)", name = "read_in_miss_5")
    readSpec(t, predicate = "a = 1 OR a IS NULL")
    readSpec(t, predicate = "a = 1 AND a IS NOT NULL")
    readSpec(t, predicate = "NOT (a = 1)")
    readSpec(t, predicate = "NOT (a IS NULL)", name = "read_not_is_null")
    readSpec(t, predicate = "a > 5", name = "read_miss_gt_5")
    readSpec(t, predicate = "a < 0", name = "read_miss_lt_0")
    snapshotSpec(t)
  }

  test("nulls_nonnulls_only") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a IS NULL")
    readSpec(t, predicate = "a IS NOT NULL")
    readSpec(t, predicate = "a = 2")
    readSpec(t, predicate = "a > 5", name = "read_miss_gt_5")
    snapshotSpec(t)
  }

  test("nulls_partial_stats") {
    sql("""CREATE TABLE tbl (a LONG, b STRING) USING delta
      TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1')""")
    sql("INSERT INTO tbl VALUES (1, 'x'), (2, 'y'), (3, 'z')")
    val t = registerTable("tbl")
    // a has stats, b does not
    readSpec(t, predicate = "a = 1")
    readSpec(t, predicate = "a > 5", name = "read_miss_a_gt_5")
    readSpec(t, predicate = "b = 'x'")
    readSpec(t, predicate = "b = 'nonexistent'", name = "read_b_no_stats")
    readSpec(t, predicate = "a = 1 AND b = 'x'")
    readSpec(t, predicate = "a > 5 AND b = 'x'", name = "read_miss_a_has_stats")
    readSpec(t, predicate = "a = 1 OR b = 'nonexistent'")
    readSpec(t, predicate = "a IS NULL", name = "read_a_is_null")
    snapshotSpec(t)
  }

  test("null_safe_eq") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "a <=> 1")
    readSpec(t, predicate = "a <=> NULL")
    readSpec(t, predicate = "a <=> 3")
    readSpec(t, predicate = "a <=> 99", name = "read_miss_nse_99")
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
    readSpec(t, predicate = "a BETWEEN 1 AND 25", name = "read_between_all")
    readSpec(t, predicate = "a BETWEEN 50 AND 100", name = "read_miss_between")
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
    readSpec(t, predicate = "a IS NULL", name = "read_miss_null")
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
    readSpec(t, predicate = "i > 100", name = "read_miss_i")
    readSpec(t, predicate = "d < 1.0", name = "read_miss_d")
    snapshotSpec(t)
  }

  test("tinyint_smallint") {
    sql("CREATE TABLE tbl (t TINYINT, s SMALLINT) USING delta")
    sql("INSERT INTO tbl VALUES (1, 100)")
    sql("INSERT INTO tbl VALUES (127, 32767)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "t = 1")
    readSpec(t, predicate = "s > 30000")
    readSpec(t, predicate = "t > 127", name = "read_miss_t")
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
    readSpec(t, predicate = "d > DATE'2025-01-01'", name = "read_miss_future")
    snapshotSpec(t)
  }

  test("timestamp_microsecond") {
    sql("CREATE TABLE tbl (ts TIMESTAMP) USING delta")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-01-01 00:00:00.000001')")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-01-01 00:00:00.000002')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "ts = TIMESTAMP'2024-01-01 00:00:00.000001'")
    readSpec(t, predicate = "ts > TIMESTAMP'2024-01-01 00:00:00.000002'",
      name = "read_miss_after")
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
      name = "read_miss_future_ntz")
    snapshotSpec(t)
  }

  test("year_function") {
    sql("CREATE TABLE tbl (d DATE, value INT) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'2024-03-15', 1), (DATE'2024-11-20', 2)")
    sql("INSERT INTO tbl VALUES (DATE'2025-01-05', 3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "year(d) = 2024")
    readSpec(t, predicate = "year(d) = 2025")
    readSpec(t, predicate = "year(d) = 2020", name = "read_miss_year")
    snapshotSpec(t)
  }

  test("month_function") {
    sql("CREATE TABLE tbl (d DATE, value INT) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'2024-01-15', 1), (DATE'2024-06-20', 2)")
    sql("INSERT INTO tbl VALUES (DATE'2024-12-05', 3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "month(d) = 1")
    readSpec(t, predicate = "month(d) = 6")
    readSpec(t, predicate = "month(d) = 8", name = "read_miss_month")
    snapshotSpec(t)
  }

  test("trunc_date") {
    sql("CREATE TABLE tbl (d DATE) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'2024-03-15'), (DATE'2024-03-20')")
    sql("INSERT INTO tbl VALUES (DATE'2024-06-01'), (DATE'2024-06-30')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "trunc(d, 'MONTH') = DATE'2024-03-01'")
    readSpec(t, predicate = "trunc(d, 'MONTH') = DATE'2024-06-01'")
    readSpec(t, predicate = "trunc(d, 'YEAR') = DATE'2025-01-01'", name = "read_miss_trunc")
    snapshotSpec(t)
  }

  test("date_trunc_timestamp") {
    sql("CREATE TABLE tbl (ts TIMESTAMP) USING delta")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-03-15 10:30:00')")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-06-01 14:00:00')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "date_trunc('MONTH', ts) = TIMESTAMP'2024-03-01 00:00:00'")
    readSpec(t, predicate = "date_trunc('YEAR', ts) = TIMESTAMP'2025-01-01 00:00:00'",
      name = "read_miss_trunc_ts")
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
      name = "read_miss_date_sub")
    snapshotSpec(t)
  }

  test("multi_file_ranges") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")   // file 1: 1-10
    sql("INSERT INTO tbl SELECT id FROM range(11, 21)")  // file 2: 11-20
    sql("INSERT INTO tbl SELECT id FROM range(21, 31)")  // file 3: 21-30
    val t = registerTable("tbl")
    readSpec(t, name = "read_full_scan")
    readSpec(t, predicate = "a <= 10", name = "read_hit_file1_only")
    readSpec(t, predicate = "a > 10 AND a <= 20", name = "read_hit_file2_only")
    readSpec(t, predicate = "a > 20", name = "read_hit_file3_only")
    readSpec(t, predicate = "a <= 15", name = "read_hit_file1_and_2")
    readSpec(t, predicate = "a > 15", name = "read_hit_file2_and_3")
    readSpec(t, predicate = "a > 100", name = "read_miss_all_gt_100")
    readSpec(t, predicate = "a < 0", name = "read_miss_all_lt_0")
    snapshotSpec(t)
  }

  test("multi_file_time") {
    sql("CREATE TABLE tbl (ts TIMESTAMP, value INT) USING delta")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-01-01 00:00:00', 1), (TIMESTAMP'2024-01-15 00:00:00', 2)")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-06-01 00:00:00', 3), (TIMESTAMP'2024-12-31 00:00:00', 4)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "ts < TIMESTAMP'2024-02-01 00:00:00'")
    readSpec(t, predicate = "ts >= TIMESTAMP'2024-06-01 00:00:00'")
    readSpec(t, predicate = "ts > TIMESTAMP'2025-01-01 00:00:00'", name = "read_miss_future")
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
    readSpec(t, predicate = "c1 > 200", name = "read_miss_c1")
    readSpec(t, predicate = "c3 > 200.0", name = "read_miss_c3")
    readSpec(t, predicate = "c4 < 1.0", name = "read_miss_c4")
    readSpec(t, predicate = "c5 > TIMESTAMP'2025-06-01 00:00:00'", name = "read_miss_c5")
    readSpec(t, predicate = "c7 > DATE'2025-01-01'", name = "read_miss_c7")
    readSpec(t, predicate = "c10 > 9.99", name = "read_miss_c10")
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
    readSpec(t, predicate = "a > 100", name = "read_a_gt_100")
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
    readSpec(t, predicate = "a > 10", name = "read_miss_a")
    // b has no stats
    readSpec(t, predicate = "b = 10")
    readSpec(t, predicate = "b > 100", name = "read_b_no_skip")
    // c has stats
    readSpec(t, predicate = "c = 100")
    readSpec(t, predicate = "c > 1000", name = "read_miss_c")
    readSpec(t, predicate = "a = 1 AND c = 100")
    readSpec(t, predicate = "a > 10 AND c > 1000", name = "read_miss_ac")
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
    readSpec(t, predicate = "a.x > 10", name = "read_miss_ax")
    // a.y no stats
    readSpec(t, predicate = "a.y = 10")
    readSpec(t, predicate = "a.y > 100", name = "read_ay_no_skip")
    // b.p no stats
    readSpec(t, predicate = "b.p = 100")
    // b.q has stats
    readSpec(t, predicate = "b.q = 1000")
    readSpec(t, predicate = "b.q > 10000", name = "read_miss_bq")
    readSpec(t, predicate = "a.x = 1 AND b.q = 1000")
    readSpec(t, predicate = "a.x > 10 AND b.q > 10000", name = "read_miss_ax_bq")
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
    readSpec(t, predicate = "a.x > 10", name = "read_miss_ax")
    readSpec(t, predicate = "a.y.z > 100", name = "read_miss_ayz")
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
    readSpec(t, predicate = "a > 5", name = "read_miss_a")
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
    readSpec(t, predicate = "b > 500", name = "read_b_no_stats")
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
    readSpec(t, predicate = "a.x > 10", name = "read_miss_ax")
    readSpec(t, predicate = "b > 1000", name = "read_miss_b")
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
    readSpec(t, predicate = "c.u > 100", name = "read_miss_cu")
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
    readSpec(t, predicate = "part = 'z'", name = "read_miss_part")
    readSpec(t, predicate = "id > 100", name = "read_miss_id")
    snapshotSpec(t)
  }

  test("partition_and_stats") {
    sql("CREATE TABLE tbl (id INT, value INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 10, 'a'), (2, 20, 'a')")
    sql("INSERT INTO tbl VALUES (3, 30, 'b'), (4, 40, 'b')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "part = 'a' AND value > 15")
    readSpec(t, predicate = "part = 'b' AND value < 25", name = "read_miss_combined")
    readSpec(t, predicate = "part = 'a' OR value > 35")
    snapshotSpec(t)
  }

  test("partition_or_predicate") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val t = registerTable("tbl")
    readSpec(t, predicate = "part = 'a' OR part = 'c'")
    readSpec(t, predicate = "part = 'z'", name = "read_miss_part")
    snapshotSpec(t)
  }

  test("schema_order_mismatch") {
    sql("CREATE TABLE tbl (a INT, b INT, c INT) USING delta")
    sql("INSERT INTO tbl VALUES (1, 2, 3)")
    val t = registerTable("tbl")
    readSpec(t, predicate = "c = 3")
    readSpec(t, columns = Seq("c", "a"))
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
    readSpec(t, predicate = "a = 5", name = "read_deleted_row")
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
    readSpec(t, predicate = "a <= 5", name = "read_all_deleted")
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
    readSpec(t, predicate = "c1 > 100", name = "read_miss_c1")
    readSpec(t, predicate = "c3 > 100.0", name = "read_miss_c3")
    readSpec(t, predicate = "c7 > DATE'2025-01-01'", name = "read_miss_c7")
    readSpec(t, predicate = "c10 > 9.99", name = "read_miss_c10")
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
    readSpec(t, predicate = "c1 > 100", name = "read_miss_c1")
    readSpec(t, predicate = "renamed_c2 = 'z'", name = "read_miss_renamed_c2")
    readSpec(t, predicate = "c3 > 100.0", name = "read_miss_c3")
    readSpec(t, predicate = "c7 > DATE'2025-01-01'", name = "read_miss_c7")
    readSpec(t, predicate = "c10 > 9.99", name = "read_miss_c10")
    readSpec(t, predicate = "c1 = 1 AND renamed_c2 = 'a'")
    readSpec(t, predicate = "c1 = 1 AND c10 = 1.23")
    readSpec(t, predicate = "c1 > 100 AND c10 > 9.99", name = "read_miss_c1_c10")
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
    readSpec(t, columns = Seq("id", "year"))
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
    readSpec(t, columns = Seq("a"))
    readSpec(t, columns = Seq("b", "d"))
    readSpec(t, columns = Seq("e", "c", "a"))
    readSpec(t, predicate = "a > 1", columns = Seq("a", "b"))
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

}
