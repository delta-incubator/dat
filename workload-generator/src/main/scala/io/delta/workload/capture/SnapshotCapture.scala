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

package io.delta.workload.capture

import java.nio.file.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, MetadataBuilder, StructField, StructType}

import io.delta.workload.deltaharness.{DeltaHarness, Metadata, Protocol}
import io.delta.workload.engine.{SnapshotResolver, SpecOutcome}
import io.delta.workload.json.JsonUtil
import io.delta.workload.model._

object SnapshotCapture {

  def capture(
      spark: SparkSession, testId: String, tablePath: Path, specsDir: Path,
      query: SnapshotQuery = SnapshotQuery(),
      expectError: ErrorExpectation = AutoDetect): Unit = {

    require(!(query.version.isDefined && query.timestamp.isDefined),
      "Cannot specify both version and timestamp")
    val version = query.version
    val timestamp = query.timestamp

    val specName = (version, timestamp) match {
      case (Some(v), _) => s"${testId}_snapshot_v$v"
      case (_, Some(ts)) => s"${testId}_snapshot_ts_${ts.replace(":", "-").replace(" ", "_")}"
      case _ => s"${testId}_snapshot"
    }
    val specPath = specsDir.resolve(s"$specName.json")

    val harness = DeltaHarness.get

    var resolvedVersion: Option[Long] = None
    val expectation: SpecExpectation[SnapshotResult] = SpecOutcome.captureExpectation {
      val log = harness.openLog(spark, tablePath.toString)
      val snapshot = SnapshotResolver.resolveSnapshot(spark, log, tablePath.toString, version, timestamp)
      if (snapshot.version < 0) {
        Failed(SpecError("DELTA_TABLE_NOT_FOUND", s"No valid Delta table at ${tablePath}"))
      } else {
        resolvedVersion = Some(snapshot.version)
        Succeeded(SnapshotResult(
          ProtocolInfo.from(snapshot.snapshot.protocol),
          MetadataInfo.from(snapshot.snapshot.metadata)))
      }
    }

    SpecOutcome.assertExpectation(expectation, specName, expectError)()

    val specVersion = version.orElse(if (timestamp.isEmpty) resolvedVersion else None)
    val spec = SnapshotSpec(SnapshotQuery(specVersion, timestamp), expectation)
    JsonUtil.writeSpec(specPath, spec)
    validateFromSpec(spark, tablePath, specPath)

    expectation match {
      case Succeeded(_) =>
        println(s"  Snapshot captured: $specName (version=${resolvedVersion.get})")
      case Failed(err) =>
        println(s"  Snapshot captured (error): $specName [${err.errorCode}] ${err.errorMessage}")
    }
  }

  /**
   * Validate a snapshot spec against the table at `tablePath`. When `isWriteValidation` is true the
   * table is a replayed, write-derived one: the per-table identifiers a replay mints fresh
   * (`id`/`name`/`description`/`createdTime`, schema column-mapping ids, and
   * `delta.columnMapping.maxColumnId`) are excluded, and everything else must still match. When
   * false (the default; the captured read-only table is the very one the spec was read from) every
   * field is compared exactly.
   */
  def validateFromSpec(
      spark: SparkSession, tablePath: Path, specPath: Path, isWriteValidation: Boolean = false): Unit = {
    val spec = JsonUtil.readSnapshotSpec(specPath)
    val specName = specPath.getFileName.toString.stripSuffix(".json")
    val harness = DeltaHarness.get
    def resolve =
      SnapshotResolver.resolveSnapshot(spark, harness.openLog(spark, tablePath.toString),
        tablePath.toString, spec.query.version, spec.query.timestamp)

    SpecOutcome.compareExpectation(spec.expectation, specName) {
      // Mirror capture's error-code derivation: version < 0 -> table-not-found code, a thrown
      // exception -> its extracted code; None means the snapshot resolved (success).
      SpecOutcome.runErrorCode {
        if (resolve.version < 0) Some("DELTA_TABLE_NOT_FOUND") else None
      }
    } { exp =>
      val live = resolve.snapshot
      assertMatches(exp.protocol, live.protocol,
        exp.metadata, live.metadata, isWriteValidation, specName)
    }
  }

  private val NonReproducibleConfigKeys = Set("delta.columnMapping.maxColumnId")
  private val ColumnMappingFieldKeys = Seq("delta.columnMapping.id", "delta.columnMapping.physicalName")

  private def assertMatches(
      expProtocol: ProtocolInfo, actProtocol: Protocol,
      expMeta: MetadataInfo, actMeta: Metadata, isWriteValidation: Boolean, name: String): Unit = {
    def requireEq(field: String, exp: Any, act: Any): Unit =
      require(exp == act, s"snapshot '$name': $field mismatch (expected $exp, got $act)")

    // ProtocolInfo.from sorts features and drops empties, so equality treats them as sets.
    requireEq("protocol", expProtocol, ProtocolInfo.from(actProtocol))
    requireEq("format", expMeta.format, actMeta.format)
    requireEq("partitionColumns", expMeta.partitionColumns, actMeta.partitionColumns)

    val expSchemaParsed = DataType.fromJson(expMeta.schemaString).asInstanceOf[StructType]
    val (expSchema, actSchema) =
      if (!isWriteValidation) (expSchemaParsed, actMeta.schema)
      else (stripColumnMappingIds(expSchemaParsed), stripColumnMappingIds(actMeta.schema))
    require(expSchema == actSchema,
      s"snapshot '$name': schema mismatch (expected ${expSchema.treeString}, got ${actSchema.treeString})")

    // Replay mints `delta.columnMapping.maxColumnId` fresh, so it's excluded for write validation.
    val cfgKeys = if (isWriteValidation) NonReproducibleConfigKeys else Set.empty[String]
    requireEq("configuration", expMeta.configuration -- cfgKeys, actMeta.configuration -- cfgKeys)

    if (!isWriteValidation) {
      requireEq("id", expMeta.id, actMeta.id)
      requireEq("name", expMeta.name, actMeta.name)
      requireEq("description", expMeta.description, actMeta.description)
      requireEq("createdTime", expMeta.createdTime, actMeta.createdTime)
    }
  }

  private def stripColumnMappingIds(schema: StructType): StructType =
    stripColumnMapping(schema).asInstanceOf[StructType]

  private def stripColumnMapping(dt: DataType): DataType = dt match {
    case st: StructType => StructType(st.fields.map { f =>
      val builder = new MetadataBuilder().withMetadata(f.metadata)
      ColumnMappingFieldKeys.foreach(builder.remove)
      StructField(f.name, stripColumnMapping(f.dataType), f.nullable, builder.build())
    })
    case at: ArrayType => at.copy(elementType = stripColumnMapping(at.elementType))
    case mt: MapType =>
      mt.copy(keyType = stripColumnMapping(mt.keyType), valueType = stripColumnMapping(mt.valueType))
    case other => other
  }

}
