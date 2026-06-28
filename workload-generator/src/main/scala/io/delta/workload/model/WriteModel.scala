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

// =============================================================================
// Low-level action types
// =============================================================================

/**
 * Add-file action serialized in a low-level `commit`. `dataFile` points to a Parquet of LOGICAL
 * rows under `data/commit_N/`; on replay the engine writes it into the table (column-mapping- and
 * partition-aware, stats computed), so physical names and stats are derived per table.
 */
@JsonPropertyOrder(Array("dataFile"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class AddFileAction(dataFile: String)

/**
 * Remove-file action (tombstone) for low-level commits. References a prior low-level add by the
 * commit ordinal that produced it (`addedAtCommit`); the engine assigns file paths per table, so
 * the tombstone is resolved to the actual path(s) at replay. The tombstone inherits the live
 * add's partitionValues/size/stats (extendedFileMetadata).
 */
@JsonPropertyOrder(Array("addedAtCommit"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class RemoveFileAction(addedAtCommit: Int)

/** Logical `rows` for a low-level add (full schema incl. partition columns); engine-materialized, not serialized. */
case class AddFileInput(rows: Seq[Map[String, Any]])

/**
 * Opaque handle for a commit returned by [[io.delta.workload.WorkloadOps.commitOp]]. Wraps the commit ordinal
 * (== table version, by the load-bearing invariant). A later `removeFiles` passes the
 * [[CommitOrdinal]] back to reference the commit that produced the files it tombstones; the typed
 * wrapper keeps it from being confused with any other count.
 */
case class CommitOrdinal(value: Int)

// AppTxn/AddDomainMetadata are the minimal spec DTOs: only what a write spec declares and
// serializes. They form their own layer, separate from io.delta.workload.log.{Txn,DomainMetadata}
// (log-fidelity parse types carrying lastUpdated/removed) and Spark's SetTransaction/DomainMetadata
// (the engine boundary).

/** Application transaction for idempotent low-level commits. */
@JsonPropertyOrder(Array("appId", "version"))
case class AppTxn(appId: String, version: Long)

/** Domain metadata entry for low-level commits (added domains). */
@JsonPropertyOrder(Array("domain", "configuration"))
case class AddDomainMetadata(
    domain: String,
    configuration: String)

// =============================================================================
// Write spec case classes
// =============================================================================

/**
 * A single commit in a write spec. Each operation is its own case class carrying only the
 * fields it uses, so illegal field combinations are unrepresentable and `match`es over the
 * hierarchy are exhaustive. The `operation` discriminator is written by Jackson as the leading
 * JSON property (`@JsonTypeInfo`); the in-memory `operation` accessor is `@JsonIgnore`d so it is
 * not also serialized as a duplicate field.
 *
 * High-level ops (create_table/replace_table/insert/update/delete/evolve_schema/
 * update_properties) replay from their parameters; the low-level [[LowLevelCommitOp]] replays its
 * recorded raw actions.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY,
  property = "operation")
@JsonSubTypes(Array(
  new JsonSubTypes.Type(value = classOf[CreateTableCommit], name = "create_table"),
  new JsonSubTypes.Type(value = classOf[ReplaceTableCommit], name = "replace_table"),
  new JsonSubTypes.Type(value = classOf[InsertCommit], name = "insert"),
  new JsonSubTypes.Type(value = classOf[DeleteCommit], name = "delete"),
  new JsonSubTypes.Type(value = classOf[UpdateCommit], name = "update"),
  new JsonSubTypes.Type(value = classOf[EvolveSchemaCommit], name = "evolve_schema"),
  new JsonSubTypes.Type(value = classOf[UpdatePropertiesCommit], name = "update_properties"),
  new JsonSubTypes.Type(value = classOf[LowLevelCommitOp], name = "commit")))
sealed trait WriteCommit {
  @JsonIgnore def operation: String
}

// ---- High-level operations (replayed from parameters) ----

@JsonPropertyOrder(Array("schema", "partitionColumns", "properties"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class CreateTableCommit(
    schema: StructType,
    partitionColumns: Option[Seq[String]] = None,
    properties: Option[Map[String, String]] = None) extends WriteCommit {
  @JsonIgnore val operation = "create_table"
}

/**
 * `CREATE OR REPLACE TABLE`: replaces schema/partitioning/properties and all data. With `rows`
 * it is a replace-as-select (RTAS): a single commit that replaces the table and writes the rows
 * (replayed via `CREATE OR REPLACE TABLE … USING delta … AS SELECT … FROM VALUES`).
 */
@JsonPropertyOrder(Array("schema", "partitionColumns", "properties", "dataFiles"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class ReplaceTableCommit(
    schema: StructType,
    partitionColumns: Option[Seq[String]] = None,
    properties: Option[Map[String, String]] = None,
    dataFiles: Option[Seq[String]] = None) extends WriteCommit {
  @JsonIgnore val operation = "replace_table"
}

@JsonPropertyOrder(Array("dataFiles"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class InsertCommit(dataFiles: Option[Seq[String]] = None) extends WriteCommit {
  @JsonIgnore val operation = "insert"
}

@JsonPropertyOrder(Array("predicate"))
case class DeleteCommit(predicate: String) extends WriteCommit {
  @JsonIgnore val operation = "delete"
}

@JsonPropertyOrder(Array("predicate", "set"))
case class UpdateCommit(predicate: String, set: Map[String, String]) extends WriteCommit {
  @JsonIgnore val operation = "update"
}

@JsonPropertyOrder(Array("addColumns", "renameColumns", "dropColumns"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class EvolveSchemaCommit(
    addColumns: Option[StructType] = None,
    renameColumns: Option[Map[String, String]] = None,
    dropColumns: Option[Seq[String]] = None) extends WriteCommit {
  @JsonIgnore val operation = "evolve_schema"
}

@JsonPropertyOrder(Array("set", "remove"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class UpdatePropertiesCommit(
    set: Option[Map[String, String]] = None,
    remove: Option[Seq[String]] = None) extends WriteCommit {
  @JsonIgnore val operation = "update_properties"
}

// ---- Low-level operation (raw Delta actions; data written via the engine on replay) ----

@JsonPropertyOrder(Array("schema", "tableProperties", "txn", "addFiles", "removeFiles",
  "addDomainMetadata", "removeDomainMetadata"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class LowLevelCommitOp(
    schema: Option[StructType] = None,
    tableProperties: Option[Map[String, String]] = None,
    txn: Option[AppTxn] = None,
    addFiles: Option[Seq[AddFileAction]] = None,
    removeFiles: Option[Seq[RemoveFileAction]] = None,
    addDomainMetadata: Option[Seq[AddDomainMetadata]] = None,
    removeDomainMetadata: Option[Seq[String]] = None) extends WriteCommit {
  @JsonIgnore val operation = "commit"
}
