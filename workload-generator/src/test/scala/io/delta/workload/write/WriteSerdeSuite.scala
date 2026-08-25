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

package io.delta.workload.write

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

import io.delta.workload.json.JsonUtil
import io.delta.workload.model._

/** Unit tests for the write-spec serde (no Spark): the WriteCommit ADT + polymorphic dispatch. */
class WriteSerdeSuite extends AnyFunSuite {

  private val mapper = JsonUtil.mapper
  private val schema = StructType(Seq(StructField("id", IntegerType), StructField("v", StringType)))

  test("WriteSpec: a mixed commit sequence round-trips, preserving each commit subtype") {
    val spec = WriteSpec(Seq(
      CreateTableCommit(schema, partitionColumns = Some(Seq("id"))),
      InsertCommit(Some(Seq("data/0.parquet"))),
      DeleteCommit("id = 1"),
      UpdateCommit("id = 2", Map("v" -> "'x'")),
      UpdatePropertiesCommit(set = Some(Map("delta.appendOnly" -> "true"))),
      EvolveSchemaCommit(addColumns = Some(StructType(Seq(StructField("c", StringType)))))))
    val rt = mapper.readValue(mapper.writeValueAsString(spec), classOf[WriteSpec])
    assert(rt == spec)
    assert(rt.commits.map(_.operation) ==
      Seq("create_table", "insert", "delete", "update", "update_properties", "evolve_schema"))
  }

  test("WriteCommit dispatch: the `operation` discriminator selects the right subtype") {
    val json = mapper.writeValueAsString(WriteSpec(Seq(DeleteCommit("x = 1"))))
    assert(mapper.readTree(json).get("commits").get(0).get("operation").asText() == "delete")
    assert(mapper.readValue(json, classOf[WriteSpec]).commits.head.isInstanceOf[DeleteCommit])
  }

  test("polymorphic Spec dispatch: a write spec deserializes to WriteSpec") {
    val json = mapper.writeValueAsString(WriteSpec(Seq.empty): Spec)
    assert(mapper.readValue(json, classOf[Spec]).isInstanceOf[WriteSpec])
  }
}
