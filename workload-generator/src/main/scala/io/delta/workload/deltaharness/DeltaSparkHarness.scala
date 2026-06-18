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

import java.nio.file.{Files, Path, StandardCopyOption}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, AddFile, DomainMetadata, FileAction, SetTransaction}
import org.apache.spark.sql.types._

import io.delta.workload.JsonUtil

class DeltaSparkHarness extends DeltaHarness {
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
      spark: SparkSession, tablePath: String,
      version: Option[Long], includePartition: Boolean): StructType = {
    val log = openLog(spark, tablePath)
    val snapshot = version.map(log.getSnapshotAt).getOrElse(log.update())
    val meta = JsonUtil.mapper.readTree(snapshot.metadataJson).get("metaData")
    val full = DataType.fromJson(meta.get("schemaString").asText()).asInstanceOf[StructType]
    if (includePartition) full
    else {
      val partCols = Option(meta.get("partitionColumns"))
        .map(_.elements().asScala.map(_.asText()).toSet).getOrElse(Set.empty[String])
      StructType(full.fields.filterNot(f => partCols.contains(f.name)))
    }
  }

  override def writeRows(
      spark: SparkSession, schema: StructType, rows: Seq[Map[String, Any]], dest: Path): Unit = {
    val sparkRows = rows.map { r =>
      Row.fromSeq(schema.fields.map(f => coerce(r.get(f.name), f.dataType)).toSeq)
    }
    val tmp = Files.createTempDirectory("row-parquet")
    try {
      spark.createDataFrame(sparkRows.asJava, schema)
        .coalesce(1).write.mode("overwrite").parquet(tmp.toString)
      val part = Files.list(tmp).iterator().asScala.find { p =>
        val n = p.getFileName.toString
        n.endsWith(".parquet") && !n.startsWith(".")
      }.getOrElse(throw new IllegalStateException(s"no parquet part-file written under $tmp"))
      Files.createDirectories(dest.getParent)
      Files.move(part, dest, StandardCopyOption.REPLACE_EXISTING)
    } finally {
      try FileUtils.deleteDirectory(tmp.toFile) catch { case _: Throwable => }
    }
  }

  override def writeRowsToTemp(
      spark: SparkSession, schema: StructType, rows: Seq[Map[String, Any]]): Path = {
    val dir = Files.createTempDirectory("row-parquet-rtas")
    val dest = dir.resolve("part-00000.parquet")
    writeRows(spark, schema, rows, dest)
    dest
  }

  /**
   * Coerce a row's value to the column's Spark type. `None`/`null` -> SQL NULL. Unknown types
   * fail loud rather than passing the raw value through: a silent passthrough that Spark happens
   * to accept could write a wrong-but-symmetric value (wrong on both capture AND replay), which
   * the row comparison would not catch — a false pass.
   */
  private def coerce(value: Option[Any], dataType: DataType): Any =
    value.flatMap(Option(_)) match {
      case None => null
      case Some(v) => dataType match {
        case _: IntegerType => v.asInstanceOf[Number].intValue()
        case _: LongType => v.asInstanceOf[Number].longValue()
        case _: ShortType => v.asInstanceOf[Number].shortValue()
        case _: ByteType => v.asInstanceOf[Number].byteValue()
        case _: DoubleType => v.asInstanceOf[Number].doubleValue()
        case _: FloatType => v.asInstanceOf[Number].floatValue()
        case _: BooleanType => v.asInstanceOf[Boolean]
        case _: StringType => v.toString
        case _: DateType => v match {
          case s: String => java.sql.Date.valueOf(s)
          case d: java.sql.Date => d
          case n: Number => new java.sql.Date(n.longValue())
          case _ => unsupported(v, dataType)
        }
        case _: TimestampType => v match {
          case s: String => java.sql.Timestamp.valueOf(s)
          case t: java.sql.Timestamp => t
          case n: Number => new java.sql.Timestamp(n.longValue())
          case _ => unsupported(v, dataType)
        }
        case _: TimestampNTZType => v match {
          case s: String => java.time.LocalDateTime.parse(s.replace(' ', 'T'))
          case ldt: java.time.LocalDateTime => ldt
          case _ => unsupported(v, dataType)
        }
        case _: DecimalType => v match {
          case bd: java.math.BigDecimal => bd
          case bd: scala.math.BigDecimal => bd.bigDecimal
          case s: String => new java.math.BigDecimal(s)
          case n: Number => new java.math.BigDecimal(n.toString)
          case _ => unsupported(v, dataType)
        }
        case _: BinaryType => v match {
          case b: Array[Byte] => b
          case s: String => java.util.Base64.getDecoder.decode(s)
          case _ => unsupported(v, dataType)
        }
        case _ => unsupported(v, dataType)
      }
    }

  private def unsupported(v: Any, dataType: DataType): Nothing =
    throw new IllegalArgumentException(
      s"coerce: unsupported value '$v' (${v.getClass.getName}) for column type $dataType")
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
