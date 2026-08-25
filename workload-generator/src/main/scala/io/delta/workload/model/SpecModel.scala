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

package io.delta.workload.model

import com.fasterxml.jackson.annotation._
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

import io.delta.workload.deltaharness.{Format, Metadata}
import io.delta.workload.json.{ReadSpecDeserializer, ReadSpecSerializer, SnapshotSpecDeserializer, SnapshotSpecSerializer}

// =============================================================================
// Spec Expected types: success data or error info
// =============================================================================

/** Common error type for all specs. */
@JsonPropertyOrder(Array("errorCode", "errorMessage"))
case class SpecError(errorCode: String, errorMessage: String)

/** Read success outcome. */
@JsonPropertyOrder(Array("rowCount", "fileCount", "filesSkipped"))
case class ReadResult(rowCount: Long, fileCount: Int, filesSkipped: Long)

/**
 * Typed on-disk metadata for a snapshot expectation. `schemaString` is the Delta schema JSON string
 * (parsed back to a `StructType` at validation time).
 */
@JsonPropertyOrder(Array("id", "name", "description", "format", "schemaString",
  "partitionColumns", "configuration", "createdTime"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class MetadataInfo(
    id: String,
    name: Option[String] = None,
    description: Option[String] = None,
    format: Format,
    schemaString: String,
    partitionColumns: Seq[String],
    configuration: Map[String, String],
    createdTime: Option[Long] = None)

object MetadataInfo {
  def from(m: Metadata): MetadataInfo =
    MetadataInfo(m.id, m.name, m.description, m.format, m.schema.json, m.partitionColumns,
      m.configuration, m.createdTime)
}

/** Snapshot success outcome. */
@JsonPropertyOrder(Array("protocol", "metadata"))
case class SnapshotResult(protocol: ProtocolInfo, metadata: MetadataInfo)

// =============================================================================
// Shared read/snapshot query types
//
// One data definition each for the read and snapshot query parameters, shared by the on-disk model
// here, the `*SpecConfig` declarations, and the DSL. The timestamp is a `String` everywhere: the
// DSL formats the declared `Instant` to the wall-clock string at declaration time.
// =============================================================================

/** Read query: optional time travel plus a predicate / column projection. */
case class ReadQuery(
    version: Option[Long] = None,
    timestamp: Option[String] = None,
    predicate: Option[String] = None,
    columns: Option[Seq[String]] = None)

/** Snapshot query: optional time travel only. */
case class SnapshotQuery(
    version: Option[Long] = None,
    timestamp: Option[String] = None)

// =============================================================================
// Spec case classes
//
// On-disk shape: top-level `version`/`timestamp` time travel, and the outcome as `expected`
// (success) XOR `error` (failure), exactly one of the two. The validator decides
// captured-vs-replayed per directory.
// =============================================================================

// Sealed umbrella over the spec types, tagged on `type`. The subtype serializers write `type`, so
// Jackson uses it as the EXISTING_PROPERTY discriminator and readValue[Spec] dispatches to the
// right case class.
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY,
  property = "type", visible = true)
@JsonSubTypes(Array(
  new JsonSubTypes.Type(value = classOf[ReadSpec], name = "read"),
  new JsonSubTypes.Type(value = classOf[SnapshotSpec], name = "snapshot"),
  new JsonSubTypes.Type(value = classOf[WriteSpec], name = "write")))
sealed trait Spec { def `type`: String }

@JsonSerialize(using = classOf[ReadSpecSerializer])
@JsonDeserialize(using = classOf[ReadSpecDeserializer])
case class ReadSpec(query: ReadQuery, expectation: SpecExpectation[ReadResult]) extends Spec {
  val `type`: String = "read"
}

@JsonSerialize(using = classOf[SnapshotSpecSerializer])
@JsonDeserialize(using = classOf[SnapshotSpecDeserializer])
case class SnapshotSpec(query: SnapshotQuery, expectation: SpecExpectation[SnapshotResult])
    extends Spec {
  val `type`: String = "snapshot"
}

// `WriteSpec` must live beside the sealed `Spec` trait (Scala requires sealed subtypes in the same
// file); the rest of the write model — the `WriteCommit` ADT and the low-level action types — lives
// in WriteModel.scala.
@JsonPropertyOrder(Array("type", "commits"))
case class WriteSpec(commits: Seq[WriteCommit]) extends Spec {
  val `type`: String = "write"
}

/** The spec outcome: `Succeeded { expected } | Failed { error }`. */
sealed trait SpecExpectation[+R]
case class Succeeded[+R](result: R) extends SpecExpectation[R]
case class Failed(error: SpecError) extends SpecExpectation[Nothing]

/**
 * Capture-time assertion about whether a read/snapshot op must fail. Not serialized (the on-disk
 * spec records the captured outcome via `expected`/`error`):
 *   - [[AutoDetect]]: no assertion; record whatever the op did (success or error).
 *   - [[AnyError]]: assert the op failed (any error code).
 *   - [[ErrorCode]]: assert the op failed AND the normalized error code matches `code`.
 */
sealed trait ErrorExpectation
case object AutoDetect extends ErrorExpectation
case object AnyError extends ErrorExpectation
case class ErrorCode(code: String) extends ErrorExpectation
