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
 * Inserts into column-mapping tables (`name` and `id` modes), including columns whose logical names
 * carry spaces and dots that only column mapping permits. Physical column names differ from logical
 * names on disk. The `delta.*` keys below are the public OSS Delta property names.
 */
class WriteColumnMappingSuite extends WorkloadTestSuite("write_column_mapping") {

  test("name_mode_insert") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("name", StringType),
      properties = Map("delta.columnMapping.mode" -> "name"))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice"), Map("id" -> 2, "name" -> "bob")))
    insertOp(w, Seq(Map("id" -> 3, "name" -> "cara")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "name = 'bob'", name = Some("read_bob"))
    snapshotSpec(t)
  }

  test("id_mode_insert") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("v", IntegerType),
      properties = Map("delta.columnMapping.mode" -> "id"))
    insertOp(w, (1 to 4).map(i => Map("id" -> i, "v" -> i * 100)))
    insertOp(w, Seq(Map("id" -> 5, "v" -> 500)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "v > 250", name = Some("read_large_v"))
    snapshotSpec(t)
  }

  test("special_character_column_names") {
    // Spaces and dots in logical names are only legal under column mapping; the physical names on
    // disk are mapping-generated. Reads reference the logical names with backtick escaping.
    val w = createTableOp("tbl",
      schema = new StructType()
        .add("id", IntegerType)
        .add("first name", StringType)
        .add("addr.city", StringType),
      properties = Map("delta.columnMapping.mode" -> "name"))
    insertOp(w, Seq(
      Map("id" -> 1, "first name" -> "ada", "addr.city" -> "london"),
      Map("id" -> 2, "first name" -> "grace", "addr.city" -> "new york")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "`first name` = 'ada'", name = Some("read_ada"))
    readSpec(t, predicate = "`addr.city` = 'london'", name = Some("read_london"))
    snapshotSpec(t)
  }

  test("upgrade_none_to_name_then_insert") {
    // Start with mapping off, upgrade the mode to `name` in place, then insert under the upgraded
    // table. The upgrade also raises the reader/writer protocol versions.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("v", IntegerType))
    insertOp(w, Seq(Map("id" -> 1, "v" -> 10), Map("id" -> 2, "v" -> 20)))
    setPropertiesOp(w, Map(
      "delta.columnMapping.mode" -> "name",
      "delta.minReaderVersion" -> "2",
      "delta.minWriterVersion" -> "5"))
    insertOp(w, Seq(Map("id" -> 3, "v" -> 30)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_before_upgrade"))
    readSpec(t, predicate = "v >= 20", name = Some("read_ge_20"))
    snapshotSpec(t)
  }
}
