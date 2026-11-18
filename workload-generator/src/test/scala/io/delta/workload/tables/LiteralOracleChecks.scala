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

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StructType}

import io.delta.workload.{TableHandle, WorkloadContext, WorkloadTestSuite}
import io.delta.workload.engine.RowComparison
import io.delta.workload.model.Succeeded

/**
 * Read and snapshot checks against literal rows and an independent schema, shared by the
 * temporal statistics and partition-value workloads. Row oracles are never read from Delta.
 */
trait LiteralOracleChecks { this: WorkloadTestSuite =>

  protected def outputDir: Path

  /**
   * Declares a read whose captured rows must equal the literal `rows`. The `all` oracle is
   * also written to `oracle-parquet/<workload>` next to the output directory, for physical
   * Parquet checks in later assertions.
   */
  protected def checkLiteralRead(
      table: TableHandle, name: String, expectedSchema: StructType, rows: Seq[Row],
      predicate: String = null, version: java.lang.Long = null): Unit = {
    val workload = WorkloadContext.current.workloadName
    readSpec(table, predicate = predicate, version = version, name = Some(name)).assert { spec =>
      spec.expectation match {
        case Succeeded(result) => require(result.rowCount == rows.size)
        case other => fail(s"$name must succeed against the independent oracle: $other")
      }
      val expected = spark.createDataFrame(rows.asJava, expectedSchema)
      val captured = outputDir.resolve(workload).resolve("expected")
        .resolve(s"${workload}_$name").resolve("expected_data")
      RowComparison.assertRowsEqual(expected, spark.read.parquet(captured.toString), name)
      if (name == "all") {
        expected.coalesce(1).write.mode("overwrite")
          .parquet(outputDir.resolveSibling("oracle-parquet").resolve(workload).toString)
      }
      println(s"Verified independent literal rows/schema: $workload/$name rows=${rows.size}")
    }
  }

  protected def checkLiteralSnapshot(
      table: TableHandle, expectedSchema: StructType, partitions: Seq[String] = Seq.empty,
      version: java.lang.Long = null): Unit = {
    snapshotSpec(table, version = version).assert { spec =>
      spec.expectation match {
        case Succeeded(result) =>
          require(DataType.fromJson(result.metadata.schemaString) == expectedSchema)
          require(result.metadata.partitionColumns == partitions)
          require(result.metadata.format.provider == "parquet")
          require(result.metadata.format.options.isEmpty)
          val ntz = expectedSchema.fields.exists(_.dataType ==
            org.apache.spark.sql.types.TimestampNTZType)
          require(result.protocol.minReaderVersion == (if (ntz) 3 else 1))
          require(result.protocol.minWriterVersion == (if (ntz) 7 else 2))
        case other => fail(s"Snapshot must succeed against independent metadata: $other")
      }
    }
  }

  /** Removes version checksums, which can carry file actions from before a mutation. */
  protected def removeVersionChecksums(dir: Path): Unit = {
    val files = Files.list(dir.resolve("_delta_log"))
    try files.iterator().asScala.filter(_.toString.endsWith(".crc"))
      .foreach(Files.delete(_)) finally files.close()
  }
}
