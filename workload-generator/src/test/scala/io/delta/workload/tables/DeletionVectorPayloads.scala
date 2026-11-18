/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.workload.tables

import java.io.DataInputStream
import java.nio.file.{Files, Path, Paths}

import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.scalatest.Assertions._

import io.delta.workload.log.DeletionVector

/** Reads the exact serialized bitmap of an inline or UUID-relative deletion vector. */
object DeletionVectorPayloads {

  def descriptor(dv: DeletionVector): DeletionVectorDescriptor = {
    // At this DAT pin USE_LONG_FOR_INTS boxes Option[Int] offsets as Long. Normalize
    // only this descriptor input, without changing the shared mapper or DV bytes.
    val offset = dv.offset.asInstanceOf[Option[Number]].map(n => Math.toIntExact(n.longValue()))
    DeletionVectorDescriptor(dv.storageType, dv.pathOrInlineDv, offset,
      dv.sizeInBytes, dv.cardinality)
  }

  def sidecar(table: Path, dv: DeletionVector): Path =
    Paths.get(descriptor(dv).absolutePath(new HadoopPath(table.toUri)).toUri.getPath)

  def payload(table: Path, dv: DeletionVector): Array[Byte] = {
    if (dv.storageType == "i") descriptor(dv).inlineData
    else {
      assert(dv.storageType == "u")
      val file = sidecar(table, dv)
      assert(file.normalize().startsWith(table.normalize()))
      val offset = descriptor(dv).offset.get
      assert(offset > 0)
      assert(Files.size(file) >= offset.toLong + 4L + dv.sizeInBytes + 4L)
      val input = new DataInputStream(Files.newInputStream(file))
      try {
        assert(input.readByte() == DeletionVectorStore.DV_FILE_FORMAT_VERSION_ID_V1)
        input.skipNBytes(offset.toLong - 1L)
        // This OSS reader checks both the payload length and checksum.
        DeletionVectorStore.readRangeFromStream(input, dv.sizeInBytes)
      } finally input.close()
    }
  }
}
