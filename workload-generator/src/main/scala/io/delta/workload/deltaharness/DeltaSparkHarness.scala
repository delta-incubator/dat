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

import scala.collection.mutable

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, Snapshot => DeltaSnapshot}
import org.apache.spark.sql.delta.actions.{Action, AddFile, DomainMetadata, FileAction, SetTransaction}
// Hide Spark's types.Metadata so the unqualified `Metadata` is this package's typed case class.
import org.apache.spark.sql.types.{Metadata => _, _}

import io.delta.workload.model.{AddDomainMetadata, AppTxn}

class DeltaSparkHarness extends SparkRowMaterializer {
  override def openLog(spark: SparkSession, tablePath: String): LogView = {
    DeltaLog.clearCache()
    new DeltaSparkLogView(DeltaLog.forTable(spark, tablePath))
  }

  override def commit(spark: SparkSession, tablePath: String, req: CommitRequest): Seq[String] = {
    DeltaLog.clearCache()
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    val txn = deltaLog.startTransaction()
    val now = System.currentTimeMillis()

    // Metadata (schema/properties) is updated first so writeFiles writes against the new schema.
    if (req.schemaJson.isDefined || req.properties.isDefined) {
      val current = txn.metadata
      txn.updateMetadata(current.copy(
        schemaString = req.schemaJson.getOrElse(current.schemaString),
        configuration = req.properties.map(current.configuration ++ _).getOrElse(current.configuration)))
    }

    // Engine-written data files: column mapping / partitioning / stats all handled by writeFiles.
    // Commit ALL file actions it returns (AddFile + AddCDCFile when change data feed is enabled);
    // only the AddFile paths are returned (for ordinal-based remove resolution).
    val fileActions: Seq[FileAction] = req.addDataParquet.flatMap { p =>
      txn.writeFiles(spark.read.parquet(p))
    }

    val manual = mutable.ArrayBuffer[Action]()
    req.setTransaction.foreach { t => manual += SetTransaction(t.appId, t.version, Some(now)) }

    if (req.removeFiles.nonEmpty) {
      // Tombstone the matching ACTIVE file so the RemoveFile inherits its partitionValues/size/
      // stats (extendedFileMetadata) — column-mapping- and partition-correct, derived not guessed.
      val active = txn.snapshot.allFiles.collect().map(a => a.path -> a).toMap
      req.removeFiles.foreach { rf =>
        val add = active.getOrElse(rf.path, throw new IllegalStateException(
          s"removeFiles references a path not active in the table: ${rf.path}"))
        manual += add.removeWithTimestamp(now, rf.dataChange)
      }
    }

    req.addDomainMetadata.foreach { dm => manual += DomainMetadata(dm.domain, dm.configuration, removed = false) }
    req.removeDomainMetadata.foreach { domain => manual += DomainMetadata(domain, "", removed = true) }

    txn.commit(fileActions ++ manual.toSeq, DeltaOperations.ManualUpdate)
    DeltaLog.clearCache()
    fileActions.collect { case a: AddFile => a.path }
  }

  override def schemaAt(
      spark: SparkSession, tablePath: String, version: Option[Long]): StructType = {
    val log = openLog(spark, tablePath)
    val snapshot = version.map(log.getSnapshotAt).getOrElse(log.update())
    snapshot.snapshot.metadata.schema
  }
}

private class DeltaSparkLogView(inner: DeltaLog) extends LogView {
  override def update(): ResolvedSnapshot = new DeltaSparkResolvedSnapshot(inner.update())
  override def getSnapshotAt(version: Long): ResolvedSnapshot =
    new DeltaSparkResolvedSnapshot(inner.getSnapshotAt(version))
  override def checkpoint(): Unit = inner.checkpoint()
  override def checkpoint(version: Long): Unit = inner.checkpoint(inner.getSnapshotAt(version))
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

  override def domainMetadata: Seq[AddDomainMetadata] =
    inner.domainMetadata.toSeq.map(dm => AddDomainMetadata(dm.domain, dm.configuration))

  override def setTransactions: Seq[AppTxn] =
    inner.setTransactions.toSeq.map(t => AppTxn(t.appId, t.version))
}
