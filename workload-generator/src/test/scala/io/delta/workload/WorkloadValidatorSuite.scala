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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Verifies [[WorkloadValidator]] can re-validate a workload tree produced by
 * [[WorkloadGenerator]] as a standalone step — without any capture-time state.
 * Also verifies it surfaces failures when the tree diverges from its specs.
 */
class WorkloadValidatorSuite extends AnyFunSuite with BeforeAndAfterAll with WorkloadOps {

  @transient private var _spark: SparkSession = _
  override def spark: SparkSession =
    try super.spark catch { case _: IllegalArgumentException => _spark }

  private var outputDir: Path = _
  private var warehouseDir: Path = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    outputDir = Files.createTempDirectory("validator-test-")
    warehouseDir = Files.createTempDirectory("validator-warehouse-")
    _spark = SparkSession.builder()
      .master("local[2]")
      .appName("WorkloadValidatorSuite")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir.toString)
      .config("spark.ui.enabled", "false")
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
      .config("spark.databricks.delta.log.cacheSize", "0")
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:${warehouseDir}/metastore_db;create=true")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (_spark != null) _spark.stop()
    Seq(outputDir, warehouseDir).foreach(d =>
      if (d != null && Files.exists(d)) FileUtils.deleteDirectory(d.toFile))
    super.afterAll()
  }

  /** Generate a single workload under [[outputDir]]. */
  private def generate(name: String)(body: => Unit): Unit = {
    val ctx = new WorkloadContext(_spark, name, Seq.empty)
    try {
      WorkloadContext.withContext(ctx) {
        body
        if (ctx.tableSpecs.size == 1) ctx.tableSpecs.head.resolveOutputName(name)
        for (ts <- ctx.tableSpecs) {
          WorkloadGenerator.generateTable(_spark, ts, outputDir)
        }
      }
    } finally {
      ctx.cleanup()
    }
  }

  test("validates a simple workload end-to-end") {
    spark.sql("DROP TABLE IF EXISTS tbl")
    generate("val_happy") {
      sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
      sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      val t = registerTable("tbl")
      readSpec(t)
      snapshotSpec(t)
    }

    val result = WorkloadValidator.validateAll(spark, outputDir)
    assert(result.success,
      s"validator should have passed; errors:\n  ${result.errors.mkString("\n  ")}")
    assert(result.passed >= 2,
      s"expected >= 2 specs validated, got ${result.passed}")
  }

  test("validates a write workload (write-derived dispatch + replay)") {
    spark.sql("DROP TABLE IF EXISTS wtbl")
    generate("val_write") {
      val w = createTableOp("wtbl", schema = "id INT, name STRING")
      insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
      commitOp(w, addFiles = Some(Seq(AddFileInput(rows = Seq(Map("id" -> 2, "name" -> "bob"))))))
      val t = registerWriteSpec(w)
      readSpec(t, name = "read_all")
      snapshotSpec(t)
    }

    // Validate the write workload standalone (post-generation): the write spec replays into a
    // fresh table and the write-derived read/snapshot specs validate against it.
    val result = WorkloadValidator.validateTestDir(spark, outputDir.resolve("val_write"))
    assert(result.success,
      s"write workload should validate; errors:\n  ${result.errors.mkString("\n  ")}")
    assert(result.passed >= 3,
      s"expected >= 3 specs (write + read + snapshot), got ${result.passed}")
  }

  test("validator reports failures when the table diverges from the spec") {
    spark.sql("DROP TABLE IF EXISTS tbl2")
    generate("val_divergence") {
      sql("CREATE TABLE tbl2 (id INT) USING delta")
      sql("INSERT INTO tbl2 VALUES (1), (2), (3), (4)")
      val t = registerTable("tbl2")
      readSpec(t)
    }

    // Corrupt the generated table — truncate the data file so the re-read
    // sees different content than what was captured.
    val tablePath = outputDir.resolve("val_divergence").resolve("delta")
    import scala.collection.JavaConverters._
    val stream = Files.list(tablePath)
    try {
      stream.iterator().asScala
        .filter(_.getFileName.toString.endsWith(".parquet"))
        .foreach(Files.delete)
    } finally stream.close()

    val result = WorkloadValidator.validateAll(spark, outputDir)
    assert(!result.success, "expected validation to fail against corrupted table")
    assert(result.errors.exists(_.contains("val_divergence")),
      s"expected error to mention val_divergence, got ${result.errors}")
  }
}
