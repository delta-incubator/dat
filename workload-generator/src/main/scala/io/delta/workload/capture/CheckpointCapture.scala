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

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession

import io.delta.workload.deltaharness.DeltaHarness
import io.delta.workload.json.JsonUtil
import io.delta.workload.model.CheckpointSpec

/**
 * Captures and validates checkpoint specs. A checkpoint spec is a bare trigger: it names a version
 * the engine under test must checkpoint.
 */
object CheckpointCapture {

  /** Force a checkpoint at `version` and emit its spec; returns the spec path. */
  def capture(
      spark: SparkSession, testId: String, tablePath: Path, specsDir: Path,
      version: Long, name: String): Path = {

    val specName = s"${testId}_$name"
    val specPath = specsDir.resolve(s"$specName.json")

    DeltaHarness.get.openLog(spark, tablePath.toString).checkpoint(version)
    require(checkpointFileExists(tablePath, version),
      s"Checkpoint $specName: no checkpoint file present at version $version after checkpoint()")

    JsonUtil.writeSpec(specPath, CheckpointSpec(version))
    println(s"  Checkpoint captured: $specName (version=$version)")
    specPath
  }

  /** Assert a checkpoint file exists at the spec's version. */
  def validateFromSpec(
      spark: SparkSession, tablePath: Path, specPath: Path): Unit = {
    val spec = JsonUtil.readSpecAs(specPath, classOf[CheckpointSpec])
    val specName = specPath.getFileName.toString.stripSuffix(".json")
    require(checkpointFileExists(tablePath, spec.version),
      s"Checkpoint validation failed for $specName: no checkpoint file at version ${spec.version}; " +
        "the engine under test did not write one")
  }

  /** True iff a checkpoint file (`<version>.checkpoint*`) exists in `_delta_log`. */
  private def checkpointFileExists(tablePath: Path, version: Long): Boolean = {
    val logDir = tablePath.resolve("_delta_log")
    val prefix = f"$version%020d.checkpoint"
    val stream = Files.list(logDir)
    try stream.iterator().asScala.exists(_.getFileName.toString.startsWith(prefix))
    finally stream.close()
  }
}
