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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_json}

import io.delta.workload.deltaharness.DeltaHarness
import io.delta.workload.json.JsonUtil
import io.delta.workload.log.{CommitLog, LastCheckpointInfo, RemoveFile}
import io.delta.workload.model._

object TableInfoCapture {

  def write(
      spark: SparkSession,
      tablePath: Path,
      outputDir: Path,
      name: String,
      description: String,
      tags: Seq[String] = Seq.empty): Unit = {
    val log = DeltaHarness.get.openLog(spark, tablePath.toString)
      val snapshot = log.update()
      val meta = snapshot.snapshot.metadata
      val protocol = ProtocolInfo.from(snapshot.snapshot.protocol)

      val deltaLogDir = tablePath.resolve("_delta_log")
      require(Files.isDirectory(deltaLogDir), s"Missing _delta_log at $tablePath")

      def listLog[T](fn: Iterator[Path] => T): T = {
        val stream = Files.list(deltaLogDir)
        try fn(stream.iterator().asScala) finally stream.close()
      }

      val commitFiles = listLog(_.filter(p =>
        p.toString.endsWith(".json") && !p.toString.contains("checkpoint")).toSeq)

      val numCommits = commitFiles.size

      // Count actions and remove actions in a single pass over each commit, via the typed log.
      val (numActions, numRemoveFiles) = commitFiles.foldLeft((0L, 0L)) {
        case ((actions, removes), f) =>
          val version = f.getFileName.toString.stripSuffix(".json").toLong
          val as = CommitLog.read(tablePath, version)
          (actions + as.size, removes + as.count(_.isInstanceOf[RemoveFile]))
      }

      val lastCrcVersion = listLog(_.map(_.getFileName.toString)
        .filter(_.endsWith(".crc"))
        .flatMap { n =>
          try Some(n.stripSuffix(".crc").toLong)
          catch { case _: NumberFormatException => None }
        }.toSeq.sorted.lastOption.getOrElse(-1L))

      val (lastCheckpointVersion, numCheckpointFiles) = {
        val lastCheckpointPath = deltaLogDir.resolve("_last_checkpoint")
        if (Files.exists(lastCheckpointPath)) {
          val cp = JsonUtil.mapper.readValue(
            Files.readAllBytes(lastCheckpointPath), classOf[LastCheckpointInfo])
          (cp.version, cp.parts.getOrElse(1L).toInt)
        } else {
          val cpFiles = listLog(_.map(_.getFileName.toString)
            .filter(n => n.contains("checkpoint") && n.endsWith(".parquet")).toSeq)
          val maxVersion = cpFiles.flatMap { n =>
            try Some(n.split("\\.")(0).toLong)
            catch { case _: NumberFormatException => None }
          }.sorted.lastOption.getOrElse(-1L)
          (maxVersion, cpFiles.count(_.startsWith(f"$maxVersion%020d")))
        }
      }

      val numAddFiles = snapshot.allFiles.count()
      val sizeInBytes = if (numAddFiles == 0) 0L
        else snapshot.allFiles.selectExpr("sum(size)").first().getLong(0)

      val logInfo = LogInfo(
        numAddFiles = numAddFiles,
        numRemoveFiles = numRemoveFiles,
        sizeInBytes = sizeInBytes,
        numCommits = numCommits,
        numActions = numActions,
        lastCheckpointVersion = lastCheckpointVersion,
        lastCrcVersion = lastCrcVersion,
        numCheckpointFiles = numCheckpointFiles)

      val partCols = meta.partitionColumns
      val numDistinctPartitions = if (partCols.nonEmpty) {
        // Convert MAP to JSON string for distinct() since Spark doesn't support set ops on MAP types
        snapshot.allFiles.select(to_json(col("partitionValues"))).distinct().count()
      } else 0L

      // Liquid clustering columns, read from DESCRIBE DETAIL (0 when the table isn't clustered).
      // Guarded for older Delta that doesn't expose the `clusteringColumns` column.
      val clusteringCols = {
        val detail = spark.sql(s"DESCRIBE DETAIL delta.`${tablePath.toAbsolutePath}`").head()
        if (detail.schema.fieldNames.contains("clusteringColumns")) {
          // Spark returns a mutable ArraySeq for array columns; collection.Seq accepts it.
          Option(detail.getAs[scala.collection.Seq[String]]("clusteringColumns"))
            .map(_.size).getOrElse(0)
        } else 0
      }

      val dataLayout = DataLayoutInfo(
        numClusteringColumns = clusteringCols,
        numPartitionColumns = partCols.size,
        numDistinctPartitions = numDistinctPartitions)

      val properties = meta.configuration

      val tableInfo = TableInfo(
        name = name,
        description = description,
        schema = meta.schema,
        protocol = protocol,
        logInfo = logInfo,
        properties = properties,
        dataLayout = dataLayout,
        tags = if (tags.nonEmpty) Some(tags) else None)

      JsonUtil.writeSpec(outputDir.resolve("table_info.json"), tableInfo)
  }
}
