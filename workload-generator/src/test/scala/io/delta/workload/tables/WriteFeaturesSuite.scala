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
 * Inserts into tables created with insert-relevant table features enabled. Each case proves plain
 * appends succeed while the feature is listed in the table's protocol. The feature flags below
 * (`delta.*`) are the public OSS Delta property names.
 */
class WriteFeaturesSuite extends WorkloadTestSuite("write_features") {

  test("append_only_inserts") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("v", StringType),
      properties = Map("delta.appendOnly" -> "true"))
    insertOp(w, Seq(Map("id" -> 1, "v" -> "a"), Map("id" -> 2, "v" -> "b")))
    insertOp(w, Seq(Map("id" -> 3, "v" -> "c")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_first_append"))
    snapshotSpec(t)
  }

  test("deletion_vectors_plain_inserts") {
    // DVs enabled at create; plain inserts only (no delete needed to exercise the enabled feature).
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("v", IntegerType),
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, (1 to 4).map(i => Map("id" -> i, "v" -> i * 10)))
    insertOp(w, Seq(Map("id" -> 5, "v" -> 50)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "v > 25", name = Some("read_large_v"))
    snapshotSpec(t)
  }

  test("type_widening_inserts") {
    // Plain appends only; the DSL has no recordable ALTER COLUMN TYPE op to exercise widening.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("v", IntegerType),
      properties = Map("delta.enableTypeWidening" -> "true"))
    insertOp(w, Seq(Map("id" -> 1, "v" -> 10), Map("id" -> 2, "v" -> 20)))
    insertOp(w, Seq(Map("id" -> 3, "v" -> 30)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "v >= 20", name = Some("read_ge_20"))
    snapshotSpec(t)
  }

  test("change_data_feed_plain_inserts") {
    // CDF enabled at create; plain appends only. Appends emit no change-data files (CDF records
    // changes for updates/deletes/merges), but the feature stays listed in the protocol throughout.
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("v", IntegerType),
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(Map("id" -> 1, "v" -> 10), Map("id" -> 2, "v" -> 20)))
    insertOp(w, Seq(Map("id" -> 3, "v" -> 30)))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_first_append"))
    readSpec(t, predicate = "v >= 20", name = Some("read_ge_20"))
    snapshotSpec(t)
  }

  // No rowTracking case: materialized*ColumnName UUIDs differ per create, so the config-map
  // comparison never matches across replay.
}
