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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

/**
 * Filesystem-level read/write/mutate for Delta commit JSON files.
 *
 * Every method here operates on `_delta_log/NNNNNNNNNNNNNNNNNNNN.json` and
 * is engine-agnostic — the Delta protocol's JSON encoding is identical across
 * Delta implementations.
 */
object CommitLog {

  /** Absolute path to a specific commit file. Does not check existence. */
  def commitFile(tableDir: Path, version: Long): Path =
    tableDir.resolve("_delta_log").resolve(f"$version%020d.json")

  // ---------------------------------------------------------------------------
  // Raw (JSON string) access
  // ---------------------------------------------------------------------------

  /** Read raw JSON lines for a commit, preserving file order. */
  def readRaw(tableDir: Path, version: Long): Seq[String] = {
    Files.readAllLines(commitFile(tableDir, version), StandardCharsets.UTF_8).asScala.toSeq
  }

  /** Overwrite a commit with the given raw JSON lines. */
  def writeRaw(tableDir: Path, version: Long, lines: Seq[String]): Unit = {
    Files.write(commitFile(tableDir, version), lines.asJava, StandardCharsets.UTF_8)
  }

  // ---------------------------------------------------------------------------
  // Typed access
  // ---------------------------------------------------------------------------

  /** Read a commit as typed [[Action]]s. Unknown / malformed lines return [[RawAction]]. */
  def read(tableDir: Path, version: Long): Seq[Action] =
    readRaw(tableDir, version).map(Action.parse)

  /** Overwrite a commit with typed actions. */
  def write(tableDir: Path, version: Long, actions: Seq[Action]): Unit =
    writeRaw(tableDir, version, actions.map(_.toJson))

  /** Read → transform → write. The common case for corruption / mutation tests. */
  def mutate(tableDir: Path, version: Long)(fn: Seq[Action] => Seq[Action]): Unit =
    write(tableDir, version, fn(read(tableDir, version)))
}
