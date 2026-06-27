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

import java.nio.file.{Files, Path, Paths}

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Base class for workload generation test suites. Provides ScalaTest integration
 * with the workload generator DSL.
 *
 * The constructor argument is the workload group; it is automatically prepended
 * to each test name to form the workload directory. Below, `basic_read` becomes
 * the workload directory `reads_basic_read/`.
 *
 * {{{
 * class ReadsSuite extends WorkloadTestSuite("reads") {
 *
 *   test("basic_read") {
 *     sql("CREATE TABLE tbl (id INT) USING delta")
 *     sql("INSERT INTO tbl VALUES (1),(2),(3)")
 *     val t = registerTable("tbl")
 *     readSpec(t)
 *     snapshotSpec(t)
 *   }
 *
 * }
 * }}}
 *
 * Run with: sbt "testOnly *ReadsSuite"
 *
 * Environment variables:
 *   WORKLOAD_OUTPUT_DIR - Output directory (default: /tmp/workloads)
 */
abstract class WorkloadTestSuite(override val suiteName: String)
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with WorkloadOps {

  protected def outputDir: Path = Paths.get(
    sys.env.getOrElse("WORKLOAD_OUTPUT_DIR",
      sys.props.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads")))

  @transient protected var _spark: SparkSession = _
  private var _ctx: WorkloadContext = _

  private var _warehouseDir: java.nio.file.Path = _
  /** True when reusing an externally-provided SparkSession (e.g. a host REPL). */
  private var _externalSession: Boolean = false

  override def beforeAll(): Unit = {
    super.beforeAll()

    // If a SparkSession already exists (e.g. a host REPL, or an earlier suite in
    // the same JVM), reuse it — but still push the configs the workload DSL
    // depends on, since a builder's config() only applies when getOrCreate()
    // creates a new session.
    val existing = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    if (existing.isDefined) {
      _spark = existing.get
      _externalSession = true
      _spark.conf.set("spark.databricks.delta.allowArbitraryProperties.enabled", "true")
      _spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    } else {
      val safeName = suiteName.replace('/', '-')
      _warehouseDir = java.nio.file.Files.createTempDirectory(s"warehouse-$safeName-")
      val builder = SparkSession.builder()
        .master("local[*]")
        .appName(s"WorkloadGenerator-$suiteName")
      builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      _spark = builder
        .config("spark.sql.warehouse.dir", _warehouseDir.toString)
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.databricks.delta.log.cacheSize", "0")
        // Tests like CheckpointsSuite set `delta.checkpoint.partSize`, which Delta
        // doesn't accept unless arbitrary properties are explicitly allowed.
        .config("spark.databricks.delta.allowArbitraryProperties.enabled", "true")
        // Also needed for MERGE schema-evolution tests.
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        // Isolate metastore per suite to avoid catalog collisions in sbt test
        .config("javax.jdo.option.ConnectionURL",
          s"jdbc:derby:${_warehouseDir}/metastore_db;create=true")
        .getOrCreate()
    }
  }

  override def afterAll(): Unit = {
    // Only stop sessions we created — never stop an externally-provided session
    if (!_externalSession && _spark != null) {
      _spark.stop()
      _spark = null
    }
    if (_warehouseDir != null && java.nio.file.Files.exists(_warehouseDir)) {
      org.apache.commons.io.FileUtils.deleteDirectory(_warehouseDir.toFile)
    }
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    if (_spark != null) _spark.sql("DROP TABLE IF EXISTS tbl")
  }

  override def afterEach(): Unit = {
    if (_ctx != null) {
      _ctx.cleanup()
      _ctx = null
    }
    super.afterEach()
  }

  /**
   * Override ScalaTest's test to integrate with workload generation.
   * Each test creates tables, declares specs, then generates output.
   *
   * The workload directory name is `<suiteName>_<testName>`, so authors don't
   * need to repeat the suite prefix in every `test(...)` call. For example,
   * inside `WorkloadTestSuite("row_tracking")`, a `test("basic_read")` produces
   * a workload directory `row_tracking_basic_read/`.
   */
  override protected def test(testName: String, testTags: org.scalatest.Tag*)(
      testFun: => Any)(implicit pos: org.scalactic.source.Position): Unit = {
    val workloadName = s"${suiteName}_$testName"
    super.test(testName, testTags: _*) {
      _ctx = new WorkloadContext(_spark, workloadName, Seq.empty)
      WorkloadContext.withContext(_ctx) {
        testFun

        if (_ctx.tableSpecs.isEmpty) {
          info("No tables declared (missing registerTable()?)")
        } else {
          // Single-table tests use the workload name as directory
          if (_ctx.tableSpecs.size == 1) {
            _ctx.tableSpecs.head.resolveOutputName(workloadName)
          }

          for (ts <- _ctx.tableSpecs) {
            // Always regenerate and validate; an existing output is overwritten by generateTable.
            val testId = WorkloadGenerator.generateTable(_spark, ts, outputDir)
            info(s"Generated $testId")
          }
        }
      }
    }
  }
}
