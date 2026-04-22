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

package io.delta.workload.log

import com.fasterxml.jackson.annotation._
import com.fasterxml.jackson.databind.node.ObjectNode

import io.delta.workload.JsonUtil

/**
 * Typed representation of a single Delta log action line.
 *
 * Each subclass corresponds to one of the protocol's action types and
 * serializes to its canonical wrapped JSON (e.g. `{"add":{...}}`,
 * `{"protocol":{...}}`). [[RawAction]] is the escape hatch for unknown
 * or deliberately malformed actions.
 */
sealed trait Action {
  /** The canonical wrapped JSON representation of this action (single line). */
  def toJson: String
}

// =============================================================================
// Add / Remove / DeletionVector
// =============================================================================

@JsonPropertyOrder(Array("storageType", "pathOrInlineDv", "offset", "sizeInBytes", "cardinality"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonIgnoreProperties(ignoreUnknown = true)
case class DeletionVector(
    storageType: String,
    pathOrInlineDv: String,
    offset: Option[Int] = None,
    sizeInBytes: Int,
    cardinality: Long)

@JsonPropertyOrder(Array("path", "partitionValues", "size", "modificationTime",
  "dataChange", "stats", "tags", "deletionVector", "baseRowId", "defaultRowCommitVersion",
  "clusteringProvider"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonIgnoreProperties(ignoreUnknown = true)
case class AddFile(
    path: String,
    size: Long,
    partitionValues: Map[String, String] = Map.empty,
    modificationTime: Long = 0L,
    dataChange: Boolean = true,
    stats: Option[String] = None,
    tags: Option[Map[String, String]] = None,
    deletionVector: Option[DeletionVector] = None,
    baseRowId: Option[Long] = None,
    defaultRowCommitVersion: Option[Long] = None,
    clusteringProvider: Option[String] = None) extends Action {
  override def toJson: String = Action.wrap("add", this)
}

@JsonPropertyOrder(Array("path", "dataChange", "deletionTimestamp", "extendedFileMetadata",
  "partitionValues", "size", "stats", "tags", "deletionVector", "baseRowId",
  "defaultRowCommitVersion"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonIgnoreProperties(ignoreUnknown = true)
case class RemoveFile(
    path: String,
    dataChange: Boolean = true,
    deletionTimestamp: Option[Long] = None,
    extendedFileMetadata: Option[Boolean] = None,
    partitionValues: Option[Map[String, String]] = None,
    size: Option[Long] = None,
    stats: Option[String] = None,
    tags: Option[Map[String, String]] = None,
    deletionVector: Option[DeletionVector] = None,
    baseRowId: Option[Long] = None,
    defaultRowCommitVersion: Option[Long] = None) extends Action {
  override def toJson: String = Action.wrap("remove", this)
}

// =============================================================================
// Metadata / Protocol
// =============================================================================

@JsonPropertyOrder(Array("provider", "options"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonIgnoreProperties(ignoreUnknown = true)
case class Format(provider: String = "parquet", options: Map[String, String] = Map.empty)

@JsonPropertyOrder(Array("id", "name", "description", "format", "schemaString",
  "partitionColumns", "configuration", "createdTime"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonIgnoreProperties(ignoreUnknown = true)
case class Metadata(
    id: String,
    name: Option[String] = None,
    description: Option[String] = None,
    format: Format = Format(),
    schemaString: String,
    partitionColumns: Seq[String] = Seq.empty,
    configuration: Map[String, String] = Map.empty,
    createdTime: Option[Long] = None) extends Action {
  override def toJson: String = Action.wrap("metaData", this)
}

@JsonPropertyOrder(Array("minReaderVersion", "minWriterVersion", "readerFeatures", "writerFeatures"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonIgnoreProperties(ignoreUnknown = true)
case class Protocol(
    minReaderVersion: Int,
    minWriterVersion: Int,
    readerFeatures: Option[Set[String]] = None,
    writerFeatures: Option[Set[String]] = None) extends Action {
  override def toJson: String = Action.wrap("protocol", this)
}

// =============================================================================
// Txn / DomainMetadata / CheckpointMetadata / SidecarFile
// =============================================================================

@JsonPropertyOrder(Array("appId", "version", "lastUpdated"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonIgnoreProperties(ignoreUnknown = true)
case class Txn(
    appId: String,
    version: Long,
    lastUpdated: Option[Long] = None) extends Action {
  override def toJson: String = Action.wrap("txn", this)
}

@JsonPropertyOrder(Array("domain", "configuration", "removed"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonIgnoreProperties(ignoreUnknown = true)
case class DomainMetadata(
    domain: String,
    configuration: String,
    removed: Boolean = false) extends Action {
  override def toJson: String = Action.wrap("domainMetadata", this)
}

@JsonPropertyOrder(Array("version", "tags"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonIgnoreProperties(ignoreUnknown = true)
case class CheckpointMetadata(
    version: Long,
    tags: Option[Map[String, String]] = None) extends Action {
  override def toJson: String = Action.wrap("checkpointMetadata", this)
}

@JsonPropertyOrder(Array("path", "sizeInBytes", "modificationTime", "tags"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
@JsonIgnoreProperties(ignoreUnknown = true)
case class SidecarFile(
    path: String,
    sizeInBytes: Long,
    modificationTime: Long,
    tags: Option[Map[String, String]] = None) extends Action {
  override def toJson: String = Action.wrap("sidecar", this)
}

// =============================================================================
// Escape hatch
// =============================================================================

/**
 * For action lines we don't have a typed form of (e.g. `commitInfo`, `cdc`)
 * or deliberately malformed JSON used by corruption tests.
 */
case class RawAction(json: String) extends Action {
  override def toJson: String = json
}

// =============================================================================
// Parser
// =============================================================================

object Action {

  /** Serialize a case class as `{"<key>": <value>}` using the DAT Jackson mapper. */
  private[log] def wrap(key: String, value: Any): String = {
    val inner = JsonUtil.mapper.valueToTree[com.fasterxml.jackson.databind.JsonNode](value)
    val outer = JsonUtil.mapper.createObjectNode()
    outer.set[com.fasterxml.jackson.databind.JsonNode](key, inner)
    JsonUtil.mapper.writeValueAsString(outer)
  }

  /**
   * Parse one commit-log JSON line into a typed [[Action]]. Falls back to
   * [[RawAction]] when the wrapper key doesn't match a known type.
   */
  def parse(raw: String): Action = {
    val node = JsonUtil.mapper.readTree(raw)
    if (!node.isObject || !node.fieldNames().hasNext) return RawAction(raw)
    val key = node.fieldNames().next()
    val body = node.get(key)
    if (body == null || !body.isObject) return RawAction(raw)
    val obj = body.asInstanceOf[ObjectNode]
    try {
      key match {
        case "add"                => JsonUtil.mapper.treeToValue(obj, classOf[AddFile])
        case "remove"             => JsonUtil.mapper.treeToValue(obj, classOf[RemoveFile])
        case "metaData"           => JsonUtil.mapper.treeToValue(obj, classOf[Metadata])
        case "protocol"           => JsonUtil.mapper.treeToValue(obj, classOf[Protocol])
        case "txn"                => JsonUtil.mapper.treeToValue(obj, classOf[Txn])
        case "domainMetadata"     => JsonUtil.mapper.treeToValue(obj, classOf[DomainMetadata])
        case "checkpointMetadata" => JsonUtil.mapper.treeToValue(obj, classOf[CheckpointMetadata])
        case "sidecar"            => JsonUtil.mapper.treeToValue(obj, classOf[SidecarFile])
        case _                    => RawAction(raw)
      }
    } catch {
      case _: Exception => RawAction(raw)  // malformed — return raw for fidelity
    }
  }
}
