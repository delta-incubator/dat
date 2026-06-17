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

import com.fasterxml.jackson.databind.JsonNode
import org.scalatest.funsuite.AnyFunSuite

/**
 * Direct unit tests for [[WorkloadValidator]]'s pure comparison functions. These branches (esp.
 * the implicit-vs-explicit protocol-feature reconciliation and the column-mapping schema
 * normalization) are hard for the capture->replay oracle to provoke, because replay produces a
 * conforming table — so they are exercised here with hand-built inputs. No Spark.
 */
class WorkloadValidatorUnitSuite extends AnyFunSuite {

  private def node(json: String): JsonNode = JsonUtil.mapper.readTree(json)

  // ---- protocolViolation: version floors + effective (implied + explicit) feature supersets ----

  test("protocol: identical legacy protocols are compatible") {
    val p = node("""{"minReaderVersion":1,"minWriterVersion":2}""")
    assert(WorkloadValidator.protocolViolation(p, p).isEmpty)
  }

  test("protocol: lower replay version is a violation") {
    val exp = node("""{"minReaderVersion":1,"minWriterVersion":5}""")
    val rep = node("""{"minReaderVersion":1,"minWriterVersion":3}""")
    assert(WorkloadValidator.protocolViolation(exp, rep).exists(_.contains("minWriterVersion")))
  }

  test("protocol: version-implied feature on expected satisfied by explicit feature on replay") {
    // Expected writer v2 implies {appendOnly, invariants}; replay (3,7) lists them explicitly.
    val exp = node("""{"minReaderVersion":1,"minWriterVersion":2}""")
    val rep = node("""{"minReaderVersion":3,"minWriterVersion":7,
      "readerFeatures":[],"writerFeatures":["appendOnly","invariants","deletionVectors"]}""")
    assert(WorkloadValidator.protocolViolation(exp, rep).isEmpty)
  }

  test("protocol: explicit expected feature missing from replay is a violation") {
    val exp = node("""{"minReaderVersion":3,"minWriterVersion":7,
      "readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}""")
    val rep = node("""{"minReaderVersion":3,"minWriterVersion":7,
      "readerFeatures":[],"writerFeatures":[]}""")
    assert(WorkloadValidator.protocolViolation(exp, rep).exists(_.contains("deletionVectors")))
  }

  test("protocol: higher replay version / extra features is allowed (capability floor)") {
    val exp = node("""{"minReaderVersion":1,"minWriterVersion":2}""")
    val rep = node("""{"minReaderVersion":3,"minWriterVersion":7,
      "readerFeatures":["columnMapping"],"writerFeatures":["appendOnly","invariants","columnMapping"]}""")
    assert(WorkloadValidator.protocolViolation(exp, rep).isEmpty)
  }

  // ---- configurationViolation: only declared keys checked; removed keys must be absent ----

  private val emptyMeta = node("""{"configuration":{}}""")

  test("config: declared key present and equal is compatible") {
    val exp = node("""{"configuration":{"delta.enableChangeDataFeed":"true"}}""")
    val rep = node("""{"configuration":{"delta.enableChangeDataFeed":"true","x":"injected"}}""")
    assert(WorkloadValidator.configurationViolation(
      exp, rep, Set("delta.enableChangeDataFeed"), Set.empty).isEmpty)
  }

  test("config: declared key missing on replay is a violation") {
    val exp = node("""{"configuration":{"delta.enableChangeDataFeed":"true"}}""")
    assert(WorkloadValidator.configurationViolation(
      exp, emptyMeta, Set("delta.enableChangeDataFeed"), Set.empty)
      .exists(_.contains("delta.enableChangeDataFeed")))
  }

  test("config: removed key still present on replay is a violation") {
    val rep = node("""{"configuration":{"delta.enableChangeDataFeed":"true"}}""")
    assert(WorkloadValidator.configurationViolation(
      emptyMeta, rep, Set.empty, Set("delta.enableChangeDataFeed"))
      .exists(_.contains("delta.enableChangeDataFeed")))
  }

  test("config: engine-injected (undeclared) keys are ignored") {
    val exp = node("""{"configuration":{}}""")
    val rep = node("""{"configuration":{"delta.columnMapping.maxColumnId":"3"}}""")
    assert(WorkloadValidator.configurationViolation(exp, rep, Set.empty, Set.empty).isEmpty)
  }

  // ---- normalizedSchema: per-table column-mapping ids are not a comparison target ----

  test("schema: differing column-mapping physicalName/id normalize to equal") {
    def schema(phys: String, id: Int) =
      s"""{"schemaString":"{\\"type\\":\\"struct\\",\\"fields\\":[{\\"name\\":\\"id\\",""" +
        s"""\\"type\\":\\"integer\\",\\"nullable\\":true,\\"metadata\\":""" +
        s"""{\\"delta.columnMapping.physicalName\\":\\"$phys\\",\\"delta.columnMapping.id\\":$id}}]}"}"""
    val a = node(schema("col-aaaa", 1))
    val b = node(schema("col-bbbb", 1))
    assert(WorkloadValidator.normalizedSchema(a) == WorkloadValidator.normalizedSchema(b))
  }

  test("schema: a real difference (column name) is NOT masked") {
    val a = node("""{"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",""" +
      """\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}"}""")
    val b = node("""{"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"renamed\",""" +
      """\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}"}""")
    assert(WorkloadValidator.normalizedSchema(a) != WorkloadValidator.normalizedSchema(b))
  }

  // ---- declaredConfiguration: replace_table resets, update_properties tracks set/remove ----

  test("declaredConfiguration: replace_table resets prior declarations") {
    val spec = WriteSpec(Seq(
      CreateTableCommit(schema = Map("fields" -> Seq.empty[Any]), properties = Some(Map("a" -> "1"))),
      UpdatePropertiesCommit(set = Some(Map("b" -> "2"))),
      ReplaceTableCommit(schema = Map("fields" -> Seq.empty[Any]), properties = Some(Map("c" -> "3")))))
    val (declared, removed) = WorkloadValidator.declaredConfiguration(spec)
    assert(declared == Set("c"), s"declared=$declared")
    assert(removed.isEmpty, s"removed=$removed")
  }

  test("declaredConfiguration: unset moves a key from declared to removed") {
    val spec = WriteSpec(Seq(
      CreateTableCommit(schema = Map("fields" -> Seq.empty[Any]), properties = Some(Map("a" -> "1"))),
      UpdatePropertiesCommit(remove = Some(Seq("a")))))
    val (declared, removed) = WorkloadValidator.declaredConfiguration(spec)
    assert(declared.isEmpty, s"declared=$declared")
    assert(removed == Set("a"), s"removed=$removed")
  }
}
