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

package io.delta.workload.deltaharness

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, Snapshot => DeltaSnapshot}
// Hide Spark's types.Metadata so the unqualified `Metadata` is this package's typed case class.
import org.apache.spark.sql.types.{Metadata => _, _}

class DeltaSparkHarness extends DeltaHarness {
  override def openLog(spark: SparkSession, tablePath: String): LogView = {
    DeltaLog.clearCache()
    new DeltaSparkLogView(DeltaLog.forTable(spark, tablePath))
  }
}

private class DeltaSparkLogView(inner: DeltaLog) extends LogView {
  override def update(): ResolvedSnapshot = new DeltaSparkResolvedSnapshot(inner.update())
  override def getSnapshotAt(version: Long): ResolvedSnapshot =
    new DeltaSparkResolvedSnapshot(inner.getSnapshotAt(version))
  override def checkpoint(): Unit = inner.checkpoint()
}

private class DeltaSparkResolvedSnapshot(inner: DeltaSnapshot) extends ResolvedSnapshot {
  override def version: Long = inner.version

  override def snapshot: Snapshot = {
    val p = inner.protocol
    val m = inner.metadata
    Snapshot(
      version = inner.version,
      protocol = Protocol(
        minReaderVersion = p.minReaderVersion,
        minWriterVersion = p.minWriterVersion,
        readerFeatures = p.readerFeatures.map(_.toSeq.sorted),
        writerFeatures = p.writerFeatures.map(_.toSeq.sorted)),
      metadata = Metadata(
        id = m.id,
        name = Option(m.name),
        description = Option(m.description),
        format = Format(m.format.provider, m.format.options),
        schema = m.schema,
        partitionColumns = m.partitionColumns,
        createdTime = m.createdTime,
        configuration = m.configuration))
  }

  override def allFiles: DataFrame = {
    val ds = inner.allFiles
    val spark = ds.sparkSession
    import spark.implicits._
    ds.map(f => (f.path, f.size, f.partitionValues, f.json))
      .toDF("path", "size", "partitionValues", "json")
  }
}
