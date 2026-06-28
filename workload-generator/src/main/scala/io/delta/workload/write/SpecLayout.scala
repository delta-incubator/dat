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

package io.delta.workload.write

import java.nio.file.{Files, Path}

import io.delta.workload.log.{AddFile, CommitLog}

/**
 * Layout conventions for a workload output directory, shared by capture and replay so the
 * "commit index -> data directory" decision lives in one place, used by both
 * [[WriteSpecBuilder]] and [[io.delta.workload.write.WriteReplay]].
 */
private[workload] object SpecLayout {
  /** Relative path (from the output dir) of the data directory for commit `idx`. */
  def commitDataDir(idx: Int): String = s"data/commit_$idx"

  /** Relative path (from the output dir) of `name` under commit `idx`'s data directory. */
  def commitDataFile(idx: Int, name: String): String = s"${commitDataDir(idx)}/$name"

  /**
   * The in-table `AddFile.path`s a commit produced, read from its `_delta_log/<version>.json`.
   * Because commit index == table version, a low-level remove's `addedAtCommit` ordinal is the
   * version whose adds it tombstones, resolved here against the actual (engine-assigned) paths.
   */
  def addPathsAt(tablePath: Path, version: Int): Seq[String] =
    if (!Files.exists(CommitLog.commitFile(tablePath, version))) Seq.empty
    else CommitLog.read(tablePath, version).collect { case a: AddFile => a.path }
}
