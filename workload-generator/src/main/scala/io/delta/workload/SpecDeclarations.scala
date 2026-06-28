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

import java.nio.file.Path

import scala.collection.mutable

import io.delta.workload.json.JsonUtil
import io.delta.workload.model._
import io.delta.workload.write.WriteSpecBuilder

// ---------------------------------------------------------------------------
// SpecRef: returned by spec declaration methods for optional assertions
// ---------------------------------------------------------------------------

/**
 * Handle to a declared spec. Attach assertions that are checked after capture.
 * `T` is the typed spec class (e.g. [[ReadSpec]], [[SnapshotSpec]]).
 */
class SpecRef[T] private[workload] (
    private[workload] val config: HasAssertion[T]) {

  /**
   * Assert that the captured spec is an error (has `error`, no `expected`).
   * Works for any spec type that follows the `expected`/`error` convention.
   */
  def assertError(): SpecRef[T] = {
    config.assertion = Some { node =>
      require(node.has("error") && !node.get("error").isNull,
        s"Expected an error spec but got a success result")
    }
    this
  }

  /** Assert conditions on the captured spec using the typed case class. */
  def assert(check: T => Unit): SpecRef[T] = {
    val deserialize = config.deserialize
    config.assertion = Some { node =>
      check(deserialize(node))
    }
    this
  }
}

private[workload] trait HasAssertion[T] {
  var assertion: Option[com.fasterxml.jackson.databind.JsonNode => Unit] = None
  def deserialize: com.fasterxml.jackson.databind.JsonNode => T
}

// ---------------------------------------------------------------------------
// Internal data structures: declared specs and the generation result
// ---------------------------------------------------------------------------

private[workload] class TableDecl(
    private var _outputName: String,
    val description: String,
    val tags: Seq[String],
    val sourcePath: Path) {
  def outputName: String = _outputName
  /** Set once by the orchestrator after body execution. */
  private[workload] def resolveOutputName(name: String): Unit = { _outputName = name }
  val readSpecs = mutable.ArrayBuffer[ReadSpecConfig]()
  val snapshotSpecs = mutable.ArrayBuffer[SnapshotSpecConfig]()
  val mutations = mutable.ArrayBuffer[Path => Unit]()
  var writeBuilder: Option[WriteSpecBuilder] = None
}

private[workload] case class ReadSpecConfig(
    name: String, query: ReadQuery,
    expectError: ErrorExpectation = AutoDetect) extends HasAssertion[ReadSpec] {
  val deserialize = (n: com.fasterxml.jackson.databind.JsonNode) =>
    JsonUtil.mapper.treeToValue(n, classOf[ReadSpec])
}

private[workload] case class SnapshotSpecConfig(
    query: SnapshotQuery,
    expectError: ErrorExpectation = AutoDetect) extends HasAssertion[SnapshotSpec] {
  val deserialize = (n: com.fasterxml.jackson.databind.JsonNode) =>
    JsonUtil.mapper.treeToValue(n, classOf[SnapshotSpec])
}
