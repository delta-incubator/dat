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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{Row, SparkSession}
// Hide Spark's types.Metadata so the unqualified `Metadata` is this package's typed case class.
import org.apache.spark.sql.types.{Metadata => _, _}

/**
 * Spark-backed row materialization shared by Delta harnesses. Provides the engine-agnostic
 * `writeRows` and value coercion so each adapter doesn't reimplement them;
 * engine-specific log/commit access stays in the concrete [[DeltaHarness]].
 */
trait SparkRowMaterializer extends DeltaHarness {
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
      deleteRecursively(tmp)
    }
  }

  /**
   * Coerce a row's value to the column's Spark type. `None`/`null` -> SQL NULL. Unknown types
   * fail loud rather than passing the raw value through: a silent passthrough that Spark happens
   * to accept could write a wrong-but-symmetric value (wrong on both capture AND replay), which
   * the row comparison would not catch: a false pass.
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

  private def deleteRecursively(dir: Path): Unit =
    try {
      Files.walk(dir).iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
    } catch {
      case _: Throwable =>
    }
}
