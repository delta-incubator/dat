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
import org.apache.spark.sql.types.StructType

import io.delta.workload.deltaharness.Protocol

// =============================================================================
// TableInfo case classes
// =============================================================================

@JsonPropertyOrder(Array("minReaderVersion", "minWriterVersion", "readerFeatures", "writerFeatures"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class ProtocolInfo(
    minReaderVersion: Int,
    minWriterVersion: Int,
    readerFeatures: Option[Seq[String]] = None,
    writerFeatures: Option[Seq[String]] = None)

object ProtocolInfo {
  // The SPI already sorts the feature lists, so `from` just drops the empty `Some(Seq())`.
  def from(p: Protocol): ProtocolInfo =
    ProtocolInfo(p.minReaderVersion, p.minWriterVersion,
      p.readerFeatures.filter(_.nonEmpty), p.writerFeatures.filter(_.nonEmpty))
}

@JsonPropertyOrder(Array("numAddFiles", "numRemoveFiles", "sizeInBytes", "numCommits",
  "numActions", "lastCheckpointVersion", "lastCrcVersion", "numCheckpointFiles"))
case class LogInfo(
    numAddFiles: Long,
    numRemoveFiles: Long,
    sizeInBytes: Long,
    numCommits: Int,
    numActions: Long,
    lastCheckpointVersion: Long,
    lastCrcVersion: Long,
    numCheckpointFiles: Int)

@JsonPropertyOrder(Array("numClusteringColumns", "numPartitionColumns", "numDistinctPartitions"))
case class DataLayoutInfo(
    numClusteringColumns: Int,
    numPartitionColumns: Int,
    numDistinctPartitions: Long)

@JsonPropertyOrder(Array("name", "description", "schema", "protocol", "logInfo",
  "properties", "dataLayout", "tags"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class TableInfo(
    name: String,
    description: String,
    schema: StructType,
    protocol: ProtocolInfo,
    logInfo: LogInfo,
    properties: Map[String, String],
    dataLayout: DataLayoutInfo,
    tags: Option[Seq[String]] = None)
