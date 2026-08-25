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

package io.delta.workload.json

import java.nio.file.{Files, Path}

import io.delta.workload.model.WriteSpec

/**
 * Write-spec serde. The [[WriteSpec]] tree (and its [[io.delta.workload.model.WriteCommit]] ADT)
 * binds through Jackson's default Scala module plus the shared [[JsonUtil.mapper]] (whose StructType
 * (de)serializer renders the typed schema each commit carries); the only write-specific entry point
 * is reading a `write` spec back from disk.
 */
object WriteSerde {

  def readWriteSpec(path: Path): WriteSpec =
    JsonUtil.mapper.readValue(Files.readAllBytes(path), classOf[WriteSpec])
}
