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

import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import io.delta.workload.deltaharness.{CommitAddFile, CommitRemoveFile, CommitRequest, DeltaHarness}

/**
 * Applies a low-level `commit` (raw Delta actions) by driving the DeltaLog transaction
 * API directly. Shared by capture (`commitOp`, against the live table) and replay
 * (`WriteSpecValidator`, against a fresh table) so both produce identical actions.
 *
 * Capture and replay differ only in where source files come from (`resolveDataFile`) and what
 * in-table path each file gets (`assignPath`): capture assigns fresh names and records them so
 * the spec stays consistent with the live log; replay reuses those recorded paths so both sides
 * commit identical `AddFile.path`s.
 *
 * Deletion vectors are intentionally not honored: the spec carries no DV descriptor and
 * none is emitted here.
 */
object LowLevelCommit {

  /**
   * @param spark           active session
   * @param tablePath       table to commit against
   * @param resolveDataFile maps an `AddFileAction.dataFile` to the source Parquet on disk
   * @param assignPath      the in-table path each added file occupies (the emitted `AddFile.path`)
   * @return the effective add files, each `dataFile` set to the in-table relative path used
   */
  def apply(
      spark: SparkSession,
      tablePath: String,
      schemaDDL: Option[String],
      tableProperties: Option[Map[String, String]],
      txn: Option[AppTxn],
      addFiles: Option[Seq[AddFileAction]],
      removeFiles: Option[Seq[RemoveFileAction]],
      addDomainMetadata: Option[Seq[AddDomainMetadata]],
      removeDomainMetadata: Option[Seq[String]],
      resolveDataFile: String => Path,
      assignPath: AddFileAction => String): Seq[AddFileAction] = {
    val resolvedAdds = addFiles.getOrElse(Seq.empty).map { file =>
      val src = resolveDataFile(file.dataFile)
      require(Files.exists(src), s"commit addFiles references missing data file: $src")
      val relative = assignPath(file)
      val dest = Paths.get(tablePath).resolve(relative)
      if (dest.toAbsolutePath.normalize() != src.toAbsolutePath.normalize()) {
        Files.createDirectories(dest.getParent)
        Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING)
      }
      val add = CommitAddFile(
        path = relative,
        partitionValues = file.partitionValues.getOrElse(Map.empty),
        size = Files.size(dest),
        dataChange = file.dataChange.getOrElse(true))
      (add, file.copy(dataFile = relative))
    }

    val req = CommitRequest(
      schemaJson = schemaDDL.map(ddl => StructType.fromDDL(ddl).json),
      properties = tableProperties,
      setTransaction = txn,
      addFiles = resolvedAdds.map(_._1),
      removeFiles = removeFiles.getOrElse(Seq.empty)
        .map(f => CommitRemoveFile(f.path, f.dataChange.getOrElse(true))),
      addDomainMetadata = addDomainMetadata.getOrElse(Seq.empty),
      removeDomainMetadata = removeDomainMetadata.getOrElse(Seq.empty))
    DeltaHarness.get.commit(spark, tablePath, req)

    resolvedAdds.map(_._2)
  }
}
