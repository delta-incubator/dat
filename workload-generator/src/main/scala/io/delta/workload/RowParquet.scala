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

import java.nio.file.{Files, Path, StandardCopyOption}

import scala.jdk.CollectionConverters._

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import io.delta.workload.deltaharness.DeltaHarness

/**
 * Materializes in-memory `rows` (the workload-generator's authoring surface) into Parquet
 * files for the write spec. The API takes rows; the spec stores Parquet. Capture writes rows
 * here at generation time — no Delta-log scan — and replay reads the produced files back.
 */
object RowParquet {

  /**
   * The table's schema at `version` (latest if None). When `includePartition` is false the
   * partition columns are dropped, matching the layout of a raw data file referenced by a
   * low-level `AddFile` (partition values ride on the action, not the file).
   */
  def schemaAt(
      spark: SparkSession, tablePath: String,
      version: Option[Long], includePartition: Boolean): StructType = {
    val log = DeltaHarness.get.openLog(spark, tablePath)
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

  /**
   * Write `rows` to a single Parquet file at `dest`. Spark writes a directory of part-files, so
   * we write to a temp dir with `coalesce(1)` and move the one part-file into place.
   */
  def writeSingle(
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

  /** Write `rows` to a single Parquet file in a fresh temp directory; returns the file path. */
  def writeTemp(spark: SparkSession, schema: StructType, rows: Seq[Map[String, Any]]): Path = {
    val dir = Files.createTempDirectory("row-parquet-rtas")
    val dest = dir.resolve("part-00000.parquet")
    writeSingle(spark, schema, rows, dest)
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
      s"RowParquet.coerce: unsupported value '$v' (${v.getClass.getName}) for column type $dataType")
}
