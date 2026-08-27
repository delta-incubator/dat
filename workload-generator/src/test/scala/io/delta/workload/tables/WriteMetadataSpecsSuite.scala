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
import io.delta.workload.model.{AddDomainMetadata, AppTxn}

/**
 * Write-derived checkpoint / CRC / CDF coverage. Each case has a write spec, so the generator
 * validates these specs against the table REPLAYED by the engine-under-test.
 */
class WriteMetadataSpecsSuite extends WorkloadTestSuite("write_meta") {

  private def idName = new StructType().add("id", IntegerType).add("name", StringType)

  test("checkpoint_partitioned") {
    val schema = new StructType().add("id", IntegerType).add("part", IntegerType)
    val w = createTableOp("tbl", schema, partitionColumns = Seq("part"))
    insertOp(w, Seq(Map("id" -> 1, "part" -> 0), Map("id" -> 2, "part" -> 1))) // v1
    insertOp(w, Seq(Map("id" -> 3, "part" -> 0))) // v2
    val t = endWrite(w)
    checkpointSpec(t, version = 2)
  }

  test("checkpoint_with_set_transaction") {
    val w = createTableOp("tbl", idName)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "a"))) // v1
    commitOp(w, txn = Some(AppTxn("cp-app", 42L))) // v2
    val t = endWrite(w)
    checkpointSpec(t, version = 2)
  }

  test("checkpoint_with_domain_metadata") {
    val w = createTableOp("tbl", idName,
      properties = Map("delta.feature.domainMetadata" -> "supported"))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "a"))) // v1
    commitOp(w, addDomainMetadata = Some(Seq(AddDomainMetadata("cpDomain", "{\"k\":\"v\"}")))) // v2
    val t = endWrite(w)
    checkpointSpec(t, version = 2)
  }

  test("crc_with_deletion_vectors") {
    val w = createTableOp("tbl", idName,
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "a"), Map("id" -> 2, "name" -> "b"),
      Map("id" -> 3, "name" -> "c"))) // v1
    deleteOp(w, "id = 2") // v2 -> deletion vector
    val t = endWrite(w)
    crcSpec(t, version = 2)
  }

  test("crc_with_set_transaction") {
    val w = createTableOp("tbl", idName)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "a"))) // v1
    commitOp(w, txn = Some(AppTxn("crc-app", 7L))) // v2
    val t = endWrite(w)
    crcSpec(t, version = 2)
  }

  test("cdf_updates_and_deletes") {
    val w = createTableOp("tbl", idName,
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "a"), Map("id" -> 2, "name" -> "b"),
      Map("id" -> 3, "name" -> "c"))) // v1
    updateOp(w, "id = 1", Map("name" -> "'a2'")) // v2 -> update pre/post image change rows
    deleteOp(w, "id = 3") // v3 -> delete change rows
    val t = endWrite(w)
    cdfSpec(t, startVersion = 0L)
  }

  test("cdf_version_range") {
    val w = createTableOp("tbl", idName,
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(Map("id" -> 1, "name" -> "a"))) // v1
    insertOp(w, Seq(Map("id" -> 2, "name" -> "b"))) // v2
    insertOp(w, Seq(Map("id" -> 3, "name" -> "c"))) // v3
    val t = endWrite(w)
    cdfSpec(t, startVersion = 1L, endVersion = 2L, name = "cdf_v1_to_v2")
  }
}
