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

package io.delta.workload

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

import io.delta.workload.deltaharness.Protocol
import io.delta.workload.json.JsonUtil
import io.delta.workload.model._

/**
 * Unit tests for the on-disk spec serde (no Spark). Pins the JSON contract the kernel consumes:
 * the flat `version`/`timestamp`/`predicate`/`columns` shape, the `expected` XOR `error` outcome,
 * the polymorphic `type` dispatch, NON_ABSENT omission, and the typed conversions.
 */
class SpecSerdeSuite extends AnyFunSuite {

  private val mapper = JsonUtil.mapper
  private def str(spec: Spec): String = mapper.writeValueAsString(spec)
  private def tree(spec: Spec) = mapper.readTree(str(spec))

  test("ReadSpec: success round-trips through JSON") {
    val spec = ReadSpec(
      ReadQuery(version = Some(3), timestamp = Some("2026-01-02 03:04:05.678"),
        predicate = Some("id > 1"), columns = Some(Seq("id", "name"))),
      Succeeded(ReadResult(rowCount = 9, fileCount = 2, filesSkipped = 1)))
    assert(mapper.readValue(str(spec), classOf[ReadSpec]) == spec)
  }

  test("ReadSpec: query fields serialize FLAT (no nested `query`)") {
    val n = tree(ReadSpec(ReadQuery(version = Some(0), predicate = Some("x")),
      Succeeded(ReadResult(1, 1, 0))))
    assert(n.get("type").asText() == "read")
    assert(n.has("version") && n.has("predicate"), "time-travel/predicate must be top-level")
    assert(!n.has("query"), "query must be flattened, not nested")
    assert(n.has("expected") && !n.has("error"))
  }

  test("ReadSpec: failure writes `error`, not `expected`, and round-trips") {
    val spec = ReadSpec(ReadQuery(), Failed(SpecError("DELTA_TABLE_NOT_FOUND", "no table")))
    val n = tree(spec)
    assert(n.has("error") && !n.has("expected"))
    assert(mapper.readValue(str(spec), classOf[ReadSpec]) == spec)
  }

  test("SnapshotSpec: success round-trips (protocol + metadata)") {
    val spec = SnapshotSpec(
      SnapshotQuery(version = Some(1)),
      Succeeded(SnapshotResult(
        ProtocolInfo(3, 7, readerFeatures = Some(Seq("deletionVectors")),
          writerFeatures = Some(Seq("appendOnly", "deletionVectors"))),
        MetadataInfo(id = "abc", format = io.delta.workload.deltaharness.Format("parquet", Map.empty),
          schemaString = """{"type":"struct","fields":[]}""",
          partitionColumns = Seq.empty, configuration = Map("k" -> "v")))))
    assert(mapper.readValue(str(spec), classOf[SnapshotSpec]) == spec)
  }

  test("polymorphic dispatch: readValue[Spec] picks the subtype from `type`") {
    val read: Spec = ReadSpec(ReadQuery(), Succeeded(ReadResult(0, 0, 0)))
    val snap: Spec = SnapshotSpec(SnapshotQuery(), Failed(SpecError("E", "m")))
    assert(mapper.readValue(mapper.writeValueAsString(read), classOf[Spec]).isInstanceOf[ReadSpec])
    assert(mapper.readValue(mapper.writeValueAsString(snap), classOf[Spec]).isInstanceOf[SnapshotSpec])
  }

  test("deserialize fails loudly when a spec has neither `expected` nor `error`") {
    intercept[Exception] {
      mapper.readValue("""{"type":"read"}""", classOf[ReadSpec])
    }
  }

  test("NON_ABSENT: an empty query omits all time-travel/predicate/columns fields") {
    val n = tree(ReadSpec(ReadQuery(), Succeeded(ReadResult(0, 0, 0))))
    assert(!n.has("version") && !n.has("timestamp") && !n.has("predicate") && !n.has("columns"))
  }

  test("ProtocolInfo.from: drops empty feature lists, keeps non-empty") {
    assert(ProtocolInfo.from(Protocol(1, 2, Some(Seq.empty), Some(Seq.empty))) ==
      ProtocolInfo(1, 2, None, None))
    assert(ProtocolInfo.from(Protocol(3, 7, Some(Seq("a")), None)) ==
      ProtocolInfo(3, 7, Some(Seq("a")), None))
  }

  test("StructType serde: round-trips as the nested Delta schema JSON object") {
    val st = StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
    val json = mapper.writeValueAsString(st)
    assert(mapper.readTree(json).get("type").asText() == "struct")
    assert(mapper.readValue(json, classOf[StructType]) == st)
  }
}
