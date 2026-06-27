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

import java.nio.file.{Files, Path}

import io.delta.workload.log.{CommitInfo, CommitLog, RawAction}

/**
 * Handle to a Delta table created via SQL. Used for declaring read specs
 * (read, snapshot) and table mutations (mutateTable, modifyCommitActions).
 */
class TableHandle private[workload] (
    private[workload] val tableName: String,
    private[workload] val sourcePath: Path,
    private[workload] val ctx: WorkloadContext) {

  /**
   * Get the commit timestamp for a specific version as an [[java.time.Instant]]. The DSL formats the
   * declared `Instant` to the timestampAsOf-safe `yyyy-MM-dd HH:mm:ss.SSS` (session TZ) string at the
   * declaration edge ([[io.delta.workload.engine.SnapshotResolver.formatTimestamp]]); the query then
   * carries that String everywhere downstream.
   *
   * Mirrors what Delta's time-travel resolution actually keys off:
   *   - ICT-enabled tables: `commitInfo.inCommitTimestamp` in the commit JSON.
   *   - Non-ICT tables:     the commit JSON file's mtime.
   *
   * DESCRIBE HISTORY and `commitInfo.timestamp` both look right at a glance but diverge by a few
   * milliseconds from the file mtime on some engines; feeding those back as `timestampAsOf` raises
   * a timestamp-out-of-range error.
   */
  def getTimestampForVersion(version: Long): java.time.Instant = {
    val padded = f"$version%020d"
    val commitPath = sourcePath.resolve(s"_delta_log/$padded.json")
    require(Files.exists(commitPath), s"No commit file for version $version: $commitPath")
    val ictOpt = CommitLog.read(sourcePath, version).iterator
      .collect { case raw: RawAction => raw.json }
      .flatMap(CommitInfo.fromLine)
      .flatMap(_.inCommitTimestamp)
      .find(_ => true)
    val tsMillis = ictOpt.getOrElse(
      Files.getLastModifiedTime(commitPath).toMillis)
    java.time.Instant.ofEpochMilli(tsMillis)
  }
}
