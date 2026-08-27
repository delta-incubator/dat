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
import io.delta.workload.write.SpecLayout

/**
 * Volume-oriented inserts: many sequential commits, a high-row-count single append, and a
 * partition-fanned append that provably produces more than one Add file in a single commit. Row
 * counts stay in the thousands to keep the suite fast.
 */
class WriteVolumeSuite extends WorkloadTestSuite("write_volume") {

  test("many_sequential_appends") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("batch", IntegerType))
    // 12 sequential single-row appends -> 12 commits on top of the create.
    (1 to 12).foreach(b => insertOp(w, Seq(Map("id" -> b, "batch" -> b))))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, version = 1, name = Some("read_after_first_append"))
    readSpec(t, version = 12, name = Some("read_after_last_append"))
    readSpec(t, predicate = "batch >= 10", name = Some("read_late_batches"))
    snapshotSpec(t)
  }

  test("high_volume_single_append") {
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("v", IntegerType).add("s", StringType))
    insertOp(w, (0 until 5000).map(i => Map("id" -> i, "v" -> (i % 100), "s" -> s"row_$i")))
    val t = endWrite(w)
    readSpec(t)
    readSpec(t, predicate = "v = 50", name = Some("read_v50"))
    snapshotSpec(t)
  }

  test("multi_file_append_via_partitions") {
    // A single insert spanning multiple partition values yields one Add per partition directory,
    // giving a deterministic multi-file commit (each partition is written to its own file).
    val w = createTableOp("tbl",
      schema = new StructType().add("id", IntegerType).add("p", IntegerType).add("v", IntegerType),
      partitionColumns = Seq("p"))
    insertOp(w, (0 until 10).map(i => Map("id" -> i, "p" -> (i % 5), "v" -> i)))
    val t = endWrite(w)

    // The insert is version 1 (create is version 0); assert it produced more than one Add.
    val adds = SpecLayout.addPathsAt(t.sourcePath, 1).size
    assert(adds > 1, s"expected a multi-file append (>1 Add) at v1, got $adds")

    readSpec(t)
    readSpec(t, predicate = "p = 2", name = Some("read_p2"))
    snapshotSpec(t)
  }
}
