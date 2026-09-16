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

import java.nio.file.{Files, Path}

import org.apache.spark.sql.SparkSession

import io.delta.workload.json.JsonUtil
import io.delta.workload.model.CrcSpec

/**
 * Captures and validates crc (version-checksum) specs. A crc spec is a bare trigger: it names a
 * version at which the engine under test must have written a `<version>.crc` sidecar.
 */
object CrcCapture {

  /** @return `Some(specPath)` if a `.crc` was present and a spec emitted, `None` if none exists. */
  def capture(
      spark: SparkSession, testId: String, tablePath: Path, specsDir: Path,
      version: Long, name: String): Option[Path] = {

    val specName = s"${testId}_$name"
    if (!crcFileExists(tablePath, version)) {
      println(s"  CRC skipped: $specName (version=$version): no .crc present at that version")
      return None
    }
    val specPath = specsDir.resolve(s"$specName.json")
    JsonUtil.writeSpec(specPath, CrcSpec(version))
    println(s"  CRC captured: $specName (version=$version)")
    Some(specPath)
  }

  /** Assert a `.crc` exists at the spec's version. Its contents are validated by the engine's on-read
   *  checksum verification when the version is loaded (via the snapshot spec). */
  def validateFromSpec(
      spark: SparkSession, tablePath: Path, specPath: Path): Unit = {
    val spec = JsonUtil.readSpecAs(specPath, classOf[CrcSpec])
    val specName = specPath.getFileName.toString.stripSuffix(".json")
    require(crcFileExists(tablePath, spec.version),
      s"CRC validation failed for $specName: no .crc at version ${spec.version}; " +
        "the engine under test did not write one")
  }

  private def crcFileExists(tablePath: Path, version: Long): Boolean =
    Files.exists(tablePath.resolve("_delta_log").resolve(f"$version%020d.crc"))
}
