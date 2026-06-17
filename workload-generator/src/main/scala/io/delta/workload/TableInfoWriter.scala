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

package io.delta.workload

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_json}

import io.delta.workload.deltaharness.DeltaHarness
import io.delta.workload.log.{CommitLog, RemoveFile}

object TableInfoWriter {

  def write(
      spark: SparkSession,
      tablePath: Path,
      outputDir: Path,
      name: String,
      description: String,
      tags: Seq[String] = Seq.empty): Unit = {
    val log = DeltaHarness.get.openLog(spark, tablePath.toString)
    val snapshot = log.update()

    val metadataJson = JsonUtil.mapper.readTree(snapshot.metadataJson).get("metaData")
    val schemaObj = JsonUtil.mapper.readValue(metadataJson.get("schemaString").asText(), classOf[Any])

    val protocolJson = JsonUtil.mapper.readTree(snapshot.protocolJson).get("protocol")
    val minReaderV = protocolJson.get("minReaderVersion").asInt()
    val minWriterV = protocolJson.get("minWriterVersion").asInt()
    val readerFeatures = Option(protocolJson.get("readerFeatures"))
      .filter(!_.isNull).map(_.elements().asScala.map(_.asText()).toSeq.sorted)
    val writerFeatures = Option(protocolJson.get("writerFeatures"))
      .filter(!_.isNull).map(_.elements().asScala.map(_.asText()).toSeq.sorted)
    val protocol = ProtocolInfo(
      minReaderVersion = minReaderV,
      minWriterVersion = minWriterV,
      readerFeatures = readerFeatures.filter(_.nonEmpty),
      writerFeatures = writerFeatures.filter(_.nonEmpty))

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
        val cpNode = JsonUtil.mapper.readTree(Files.readAllBytes(lastCheckpointPath))
        val ver = cpNode.get("version").asLong()
        val numParts = Option(cpNode.get("parts")).map(_.asInt()).getOrElse(1)
        (ver, numParts)
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

    val partCols = Option(metadataJson.get("partitionColumns"))
      .filter(!_.isNull)
      .map(_.elements().asScala.map(_.asText()).toSeq)
      .getOrElse(Seq.empty)
    val numDistinctPartitions = if (partCols.nonEmpty) {
      // Convert MAP to JSON string for distinct() since Spark doesn't support set ops on MAP types
      snapshot.allFiles.select(to_json(col("partitionValues"))).distinct().count()
    } else 0L

    val dataLayout = DataLayoutInfo(
      numClusteringColumns = 0,
      numPartitionColumns = partCols.size,
      numDistinctPartitions = numDistinctPartitions)

    val properties = Option(metadataJson.get("configuration"))
      .filter(!_.isNull)
      .map { node =>
        node.fields().asScala.map(e => e.getKey -> e.getValue.asText()).toMap
      }
      .getOrElse(Map.empty)

    val tableInfo = TableInfo(
      name = name,
      description = description,
      schema = schemaObj,
      protocol = protocol,
      logInfo = logInfo,
      properties = properties,
      dataLayout = dataLayout,
      tags = if (tags.nonEmpty) Some(tags) else None)

    JsonUtil.writeSpec(outputDir.resolve("table_info.json"), tableInfo)
  }
}
