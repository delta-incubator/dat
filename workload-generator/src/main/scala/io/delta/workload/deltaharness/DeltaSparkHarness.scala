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
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, AddFile, DomainMetadata, RemoveFile, SetTransaction}

class DeltaSparkHarness extends DeltaHarness {
  override def openLog(spark: SparkSession, tablePath: String): LogView = {
    DeltaLog.clearCache()
    new DeltaSparkLogView(DeltaLog.forTable(spark, tablePath))
  }

  override def commit(spark: SparkSession, tablePath: String, req: CommitRequest): Unit = {
    DeltaLog.clearCache()
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    val transaction = deltaLog.startTransaction()
    val actions = mutable.ArrayBuffer[Action]()

    if (req.schemaJson.isDefined || req.properties.isDefined) {
      val currentMetadata = transaction.metadata
      val newSchemaString = req.schemaJson.getOrElse(currentMetadata.schemaString)
      val newConfig = req.properties
        .map(props => currentMetadata.configuration ++ props)
        .getOrElse(currentMetadata.configuration)
      transaction.updateMetadata(
        currentMetadata.copy(schemaString = newSchemaString, configuration = newConfig))
    }

    req.setTransaction.foreach { t =>
      actions += SetTransaction(t.appId, t.version, Some(System.currentTimeMillis()))
    }
    req.addFiles.foreach { f =>
      actions += AddFile(
        path = f.path,
        partitionValues = f.partitionValues,
        size = f.size,
        modificationTime = System.currentTimeMillis(),
        dataChange = f.dataChange)
    }
    req.removeFiles.foreach { f =>
      actions += RemoveFile(
        path = f.path,
        deletionTimestamp = Some(System.currentTimeMillis()),
        dataChange = f.dataChange)
    }
    req.addDomainMetadata.foreach { dm =>
      actions += DomainMetadata(dm.domain, dm.configuration, removed = false)
    }
    req.removeDomainMetadata.foreach { domain =>
      actions += DomainMetadata(domain, "", removed = true)
    }

    transaction.commit(actions.toSeq, DeltaOperations.ManualUpdate)
    DeltaLog.clearCache()
  }
}

private class DeltaSparkLogView(inner: DeltaLog) extends LogView {
  override def update(): SnapshotView = new DeltaSparkSnapshotView(inner.update())
  override def getSnapshotAt(version: Long): SnapshotView =
    new DeltaSparkSnapshotView(inner.getSnapshotAt(version))
  override def checkpoint(): Unit = inner.checkpoint()
}

private class DeltaSparkSnapshotView(inner: Snapshot) extends SnapshotView {
  override def version: Long = inner.version
  override def protocolJson: String = inner.protocol.json
  override def metadataJson: String = inner.metadata.json
  override def allFiles: DataFrame = {
    val ds = inner.allFiles
    val spark = ds.sparkSession
    import spark.implicits._
    ds.map(f => (f.path, f.size, f.partitionValues, f.json))
      .toDF("path", "size", "partitionValues", "json")
  }
}
