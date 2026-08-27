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

import org.apache.spark.sql.types._

import io.delta.workload.WorkloadTestSuite

/**
 * Insert-resolution inserts: how values map onto the target schema. The row-value API resolves each
 * column by name (not by map iteration order), fills omitted nullable columns with NULL, and picks
 * up columns added by schema evolution.
 */
class WriteResolutionSuite extends WorkloadTestSuite("write_resolution") {

  test("all_columns_present") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("name", StringType).add("v", IntegerType))
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a", "v" -> 10),
      Map("id" -> 2, "name" -> "b", "v" -> 20)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "v >= 20", name = Some("read_ge_20"))
    snapshotSpec(t)
  }

  test("reordered_row_keys_resolve_by_name") {
    // Row maps list keys in an order unrelated to the schema; values still land in the right column
    // because resolution is by name.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("name", StringType).add("v", IntegerType))
    insertOp(w, Seq(
      Map("v" -> 100, "id" -> 1, "name" -> "x"),
      Map("name" -> "y", "v" -> 200, "id" -> 2)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "name = 'x'", name = Some("read_x"))
    snapshotSpec(t)
  }

  test("missing_nullable_column_filled_with_null") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("name", StringType).add("score", IntegerType))
    // Second row omits `score`; the omitted nullable column is written as NULL.
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a", "score" -> 50),
      Map("id" -> 2, "name" -> "b")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "score IS NULL", name = Some("read_null_score"))
    readSpec(t, predicate = "score IS NOT NULL", name = Some("read_scored"))
    snapshotSpec(t)
  }

  test("insert_into_evolved_schema_new_column") {
    // A new nullable column is added via schema evolution, then a later insert supplies it. Rows
    // written before the evolution read back NULL for the new column.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("name", StringType))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "a"), Map("id" -> 2, "name" -> "b")))
    addColumnsOp(w, new StructType().add("tag", StringType))
    insertOp(w, Seq(Map("id" -> 3, "name" -> "c", "tag" -> "t3")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_before_evolution"))
    readSpec(t, predicate = "tag IS NULL", name = Some("read_null_tag"))
    readSpec(t, predicate = "tag = 't3'", name = Some("read_tag3"))
    snapshotSpec(t)
  }

  test("multi_row_single_insert") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("v", IntegerType))
    insertOp(w, (1 to 8).map(i => Map("id" -> i, "v" -> i * 5)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "v > 20", name = Some("read_big_v"))
    snapshotSpec(t)
  }

  test("column_with_comment_in_schema") {
    // A column carrying a schema COMMENT round-trips through create + insert.
    val w = createTableOp("tbl",
      schema = new StructType()
        .add("id", IntegerType)
        .add("note", StringType, nullable = true, "free-form note about the row"))
    insertOp(w, Seq(
      Map("id" -> 1, "note" -> "first"),
      Map("id" -> 2, "note" -> "second")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "note = 'first'", name = Some("read_first_note"))
    snapshotSpec(t)
  }

  test("char_column") {
    // A fixed-width CHAR(n) column: inserted values are space-padded to the declared width, and the
    // padded form must round-trip through capture->replay.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("code", CharType(5)))
    insertOp(w, Seq(
      Map("id" -> 1, "code" -> "ab"),
      Map("id" -> 2, "code" -> "abcde")))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  test("empty_insert_is_rejected") {
    // A 0-row insert produces no commit, so the append DSL rejects it rather than recording a
    // spec with nothing to validate. Documented here as the resolution boundary for empty inserts.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType))
    val ex = intercept[IllegalArgumentException] {
      insertOp(w, Seq.empty)
    }
    assert(ex.getMessage.contains("at least one row"), s"message: ${ex.getMessage}")
    // A real (non-empty) insert follows so the table has a captured commit history.
    insertOp(w, Seq(Map("id" -> 1)))
    val t = endWrite(w)
    readSpec(t)
    snapshotSpec(t)
  }

  // No INSERT ... SELECT case: the row-value API appends only supplied rows, so insert-from-self
  // isn't expressible here.
}
