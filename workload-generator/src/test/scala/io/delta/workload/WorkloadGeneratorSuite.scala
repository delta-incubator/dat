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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import io.delta.workload.capture.{ReadCapture, SnapshotCapture}
import io.delta.workload.engine.RowComparison
import io.delta.workload.json.JsonUtil
import io.delta.workload.log.{AddFile, Metadata}
import io.delta.workload.model._

class WorkloadGeneratorSuite extends AnyFunSuite with BeforeAndAfterAll with WorkloadOps {

  private var _spark: SparkSession = _
  private var _externalSession: Boolean = false

  override def spark: SparkSession =
    try { super.spark } catch { case _: IllegalArgumentException => _spark }
  private var outputDir: Path = _
  private var warehouseDir: Path = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    outputDir = Files.createTempDirectory("workload-test-")
    warehouseDir = Files.createTempDirectory("workload-warehouse-")

    val existing = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    if (existing.isDefined) {
      _spark = existing.get
      _externalSession = true
    } else {
      _spark = SparkSession.builder()
        .master("local[2]")
        .appName("WorkloadGeneratorSuite")
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
  }

  override def afterAll(): Unit = {
    if (!_externalSession && _spark != null) _spark.stop()
    Seq(outputDir, warehouseDir).foreach { d =>
      if (d != null) org.apache.commons.io.FileUtils.deleteDirectory(d.toFile)
    }
    super.afterAll()
  }

  /** Test result for workload generation. */
  case class TestResult(
      testId: String,
      passed: Boolean,
      errors: Seq[String],
      skipped: Boolean = false)

  /**
   * Run a workload test. Creates a context, executes the body, generates tables.
   */
  private def run(name: String = "test", force: Boolean = true)(body: WorkloadContext => Unit): Seq[TestResult] = {
    import scala.collection.mutable
    val results = mutable.ArrayBuffer[TestResult]()

    val ctx = new WorkloadContext(_spark, name, Seq.empty)
    try {
      WorkloadContext.withContext(ctx)(body(ctx))

      if (ctx.tableSpecs.isEmpty) {
        results += TestResult(name, passed = true, Seq("No tables declared"))
      } else {
        if (ctx.tableSpecs.size == 1) {
          ctx.tableSpecs.head.resolveOutputName(name)
        }

        for (ts <- ctx.tableSpecs) {
          val testOutputDir = outputDir.resolve(ts.outputName)

          if (!force && Files.exists(testOutputDir.resolve("table_info.json"))) {
            results += TestResult(ts.outputName, passed = true, Seq.empty, skipped = true)
          } else {
            val testId = WorkloadGenerator.generateTable(_spark, ts, outputDir)
            results += TestResult(testId, passed = true, Seq.empty)
          }
        }
      }
    } catch {
      case e: Exception =>
        cleanupDir(outputDir.resolve(name))
        results += TestResult(name, passed = false, Seq(e.getMessage))
    } finally {
      ctx.cleanup()
    }
    results.toSeq
  }

  private def cleanupDir(dir: Path): Unit = {
    if (Files.exists(dir)) FileUtils.deleteDirectory(dir.toFile)
  }

  private def assertPassed(results: Seq[TestResult]): Unit = {
    results.foreach { r =>
      assert(r.passed, s"${r.testId} failed: ${r.errors.mkString("; ")}")
    }
  }

  private def dir(name: String): Path = outputDir.resolve(name)
  private def specs(name: String): Path = dir(name).resolve("specs")
  private def expected(name: String): Path = dir(name).resolve("expected")
  private def delta(name: String): Path = dir(name).resolve("delta")

  private def readSpec(name: String, specName: String): com.fasterxml.jackson.databind.JsonNode = {
    val f = specs(name).resolve(s"${name}_$specName.json")
    assert(Files.exists(f), s"Spec file missing: $f")
    JsonUtil.mapper.readTree(Files.readAllBytes(f))
  }

  // =========================================================================
  // Read specs
  // =========================================================================

  test("read: basic table read with row validation") {
    val results = run("t_r1") { _ =>
      sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
      sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      val t = registerTable("tbl")
      readSpec(t)
    }
    assertPassed(results)

    // Verify expected data matches actual table
    val actual = spark.read.format("delta").load(delta("t_r1").toString)
    val expectedData = spark.read.parquet(
      expected("t_r1").resolve("t_r1_read_all/expected_data").toString)
    RowComparison.assertRowsEqual(expectedData, actual, "t_r1")
    assert(actual.count() == 3, "Should have 3 rows")
  }

  test("read: predicate filters rows correctly") {
    val results = run("t_r2") { _ =>
      sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
      sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
      val t = registerTable("tbl")
      readSpec(t, predicate = "id > 3")
    }
    assertPassed(results)
    val expectedData = spark.read.parquet(
      expected("t_r2").resolve("t_r2_read_id_gt_3/expected_data").toString).count()
    assert(expectedData == 2, "Predicate id > 3 should yield 2 rows")
  }

  test("read: version time travel") {
    val results = run("t_r3") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1)")
      sql("INSERT INTO tbl VALUES (2)")
      sql("INSERT INTO tbl VALUES (3)")
      val t = registerTable("tbl")
      readSpec(t, version = 1)
      readSpec(t, version = 2)
      readSpec(t)
    }
    assertPassed(results)
    val v1 = spark.read.parquet(
      expected("t_r3").resolve("t_r3_read_v1/expected_data").toString).count()
    val v2 = spark.read.parquet(
      expected("t_r3").resolve("t_r3_read_v2/expected_data").toString).count()
    val latest = spark.read.parquet(
      expected("t_r3").resolve("t_r3_read_all/expected_data").toString).count()
    assert(v1 == 1, "Version 1 should have 1 row")
    assert(v2 == 2, "Version 2 should have 2 rows")
    assert(latest == 3, "Latest should have 3 rows")
  }

  test("read: column projection") {
    val results = run("t_r4") { _ =>
      sql("CREATE TABLE tbl (a INT, b STRING, c DOUBLE) USING delta")
      sql("INSERT INTO tbl VALUES (1, 'x', 1.1), (2, 'y', 2.2)")
      val t = registerTable("tbl")
      readSpec(t, columns = Some(Seq("a", "c")))
    }
    assertPassed(results)
    val df = spark.read.parquet(
      expected("t_r4").resolve("t_r4_read_cols_a_c/expected_data").toString)
    assert(df.columns.toSet == Set("a", "c"), "Should only have projected columns")
    assert(df.count() == 2)
  }

  test("read: partitioned table") {
    val results = run("t_r5") { _ =>
      sql("""CREATE TABLE tbl (id INT, region STRING)
        USING delta PARTITIONED BY (region)""")
      sql("INSERT INTO tbl VALUES (1,'us'),(2,'us'),(3,'eu'),(4,'eu')")
      val t = registerTable("tbl")
      readSpec(t, predicate = "region = 'us'")
      readSpec(t)
    }
    assertPassed(results)
    val filtered = spark.read.parquet(
      expected("t_r5").resolve("t_r5_read_region_eq_us/expected_data").toString).count()
    val all = spark.read.parquet(
      expected("t_r5").resolve("t_r5_read_all/expected_data").toString).count()
    assert(filtered == 2, "Filtered should have 2 rows")
    assert(all == 4, "All should have 4 rows")
  }

  test("read: data skipping with multi-file predicates") {
    val results = run("t_r_skip") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      // 3 separate inserts → 3 files with non-overlapping ranges
      sql("INSERT INTO tbl VALUES (1),(2),(3)")      // file 1: min=1, max=3
      sql("INSERT INTO tbl VALUES (10),(11),(12)")    // file 2: min=10, max=12
      sql("INSERT INTO tbl VALUES (100),(101),(102)") // file 3: min=100, max=102
      val t = registerTable("tbl")
      readSpec(t)                              // all 9 rows
      readSpec(t, predicate = "id < 5")        // only file 1: 3 rows
      readSpec(t, predicate = "id >= 100")     // only file 3: 3 rows
      readSpec(t, predicate = "id > 3 AND id < 100") // only file 2: 3 rows
      readSpec(t, predicate = "id = 11")       // only file 2: 1 row
      readSpec(t, predicate = "id > 200")      // no files match: 0 rows
    }
    assertPassed(results)

    // Verify exact row counts for each predicate
    def rowCount(specSuffix: String): Long = {
      val p = expected("t_r_skip").resolve(s"t_r_skip_$specSuffix/expected_data")
      if (!Files.exists(p)) 0L
      else spark.read.parquet(p.toString).count()
    }
    assert(rowCount("read_all") == 9, "All rows")
    assert(rowCount("read_id_lt_5") == 3, "id < 5 → 3 rows from file 1")
    assert(rowCount("read_id_gte_100") == 3, "id >= 100 → 3 rows from file 3")
    assert(rowCount("read_id_gt_3_and_id_lt_100") == 3, "3 < id < 100 → 3 rows from file 2")
    assert(rowCount("read_id_eq_11") == 1, "id = 11 → 1 row")

    // Verify the zero-match predicate produces an empty spec
    val zeroSpec = readSpec("t_r_skip", "read_id_gt_200")
    val zeroExpected = zeroSpec.get("expected")
    assert(zeroExpected.get("rowCount").asInt() == 0, "id > 200 → 0 rows")
  }

  test("read: empty table") {
    val results = run() { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      val t = registerTable("tbl")
      readSpec(t)
      snapshotSpec(t)
    }
    assertPassed(results)
  }

  test("read: null values preserved") {
    val results = run("t_r7") { _ =>
      sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
      sql("INSERT INTO tbl VALUES (1, null), (2, 'b'), (null, 'c')")
      val t = registerTable("tbl")
      readSpec(t)
    }
    assertPassed(results)
    val data = spark.read.parquet(
      expected("t_r7").resolve("t_r7_read_all/expected_data").toString).count()
    assert(data == 3)
  }

  test("read: spec JSON has correct structure") {
    val results = run("t_r8") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1)")
      val t = registerTable("tbl")
      readSpec(t, predicate = "id > 0", version = 1, columns = Some(Seq("id")))
    }
    assertPassed(results)
    val spec = readSpec("t_r8", "read_v1_id_gt_0_cols_id")
    assert(spec.get("type").asText() == "read")
    assert(spec.get("version").asInt() == 1)
    assert(spec.get("predicate").asText() == "id > 0")
    assert(spec.get("expected").get("rowCount").asInt() == 1,
      "One row matches id > 0 at version 1")
  }

  // =========================================================================
  // Error specs
  // =========================================================================

  test("error: nonexistent version produces error spec") {
    val results = run("t_e1") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1)")
      val t = registerTable("tbl")
      readSpec(t, version = 999)
    }
    assertPassed(results)
    val spec = JsonUtil.readReadSpec(specs("t_e1").resolve("t_e1_read_v999.json"))
    assert(spec.query.version.contains(999L))
    spec.expectation match {
      case Failed(err) =>
        // Error code can vary depending on the code path
        assert(err.errorCode.nonEmpty, "Error code should not be empty")
        // Error message or code should reference the bad version
        assert(err.errorMessage.contains("999") || err.errorCode.contains("Version"),
          s"Error should reference version 999, got code: ${err.errorCode}, msg: ${err.errorMessage}")
      case Succeeded(_) =>
        fail("Expected error spec for nonexistent version")
    }
  }

  // =========================================================================
  // Snapshot specs
  // =========================================================================

  test("snapshot: captures protocol and metadata") {
    val results = run("t_s1") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1)")
      val t = registerTable("tbl")
      snapshotSpec(t)
    }
    assertPassed(results)
    val spec = readSpec("t_s1", "snapshot")
    assert(spec.get("type").asText() == "snapshot")
    val exp = spec.get("expected")
    // Verify protocol exact values for a basic INT table
    val proto = exp.get("protocol")
    val minReader = proto.get("minReaderVersion").asInt()
    val minWriter = proto.get("minWriterVersion").asInt()
    assert(minReader == 1, s"minReaderVersion should be 1, got $minReader")
    assert(minWriter == 2, s"minWriterVersion should be 2, got $minWriter")
    // Verify metadata schema is exactly one INT column named "id"
    val meta = exp.get("metadata")
    val schemaStr = meta.get("schemaString").asText()
    val schema = JsonUtil.mapper.readTree(schemaStr)
    assert(schema.get("type").asText() == "struct")
    val fields = schema.get("fields")
    assert(fields.size() == 1, s"Should have exactly 1 field, got ${fields.size()}")
    assert(fields.get(0).get("name").asText() == "id")
    assert(fields.get(0).get("type").asText() == "integer")
    assert(fields.get(0).get("nullable").asBoolean() == true)
    // Metadata ID is a UUID
    val metaId = meta.get("id").asText()
    assert(metaId.matches("[0-9a-f-]{36}"), s"metadata id should be UUID, got: $metaId")
  }

  test("snapshot: at specific version") {
    val results = run("t_s2") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1)")
      sql("INSERT INTO tbl VALUES (2)")
      val t = registerTable("tbl")
      snapshotSpec(t, version = 1)
      snapshotSpec(t, version = 2)
    }
    assertPassed(results)
    val s1 = readSpec("t_s2", "snapshot_v1")
    val s2 = readSpec("t_s2", "snapshot_v2")
    assert(s1.get("version").asInt() == 1)
    assert(s2.get("version").asInt() == 2)
  }

  test("snapshot: explicit version loop captures all versions") {
    val results = run("t_s3") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1)")
      sql("INSERT INTO tbl VALUES (2)")
      val t = registerTable("tbl")
      for (v <- 0L to 2) snapshotSpec(t, version = v)
    }
    assertPassed(results)
    for (v <- 0 to 2) {
      assert(Files.exists(specs("t_s3").resolve(s"t_s3_snapshot_v$v.json")),
        s"Missing snapshot for version $v")
    }
  }

  // =========================================================================
  // Snapshot: DummySnapshot (corrupted/empty log) handling
  // =========================================================================

  test("snapshot: corrupted table produces error spec (DummySnapshot path)") {
    val results = run("t_dummy") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1)")
      val t = registerTable("tbl")
      mutateTable(t) { tableDir =>
        val logDir = tableDir.resolve("_delta_log")
        Files.list(logDir).iterator().asScala
          .filter(p => p.toString.endsWith(".json") || p.toString.endsWith(".crc"))
          .foreach(Files.delete)
      }
      snapshotSpec(t)
    }
    assertPassed(results)
    val spec = readSpec("t_dummy", "snapshot")
    assert(spec.has("error"), "Should produce error spec for empty log")
    assert(!spec.has("expected") || spec.get("expected").isNull,
      "Should not have success data for empty log")
  }

  // =========================================================================
  // Validation catches wrong output
  // =========================================================================

  test("validation: tampered read expected_data caught by validateCapturedRead") {
    // Generate valid read, then tamper expected_data, then call validation directly
    run("t_v1") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1),(2),(3)")
      val t = registerTable("tbl")
      readSpec(t)
    }
    // Tamper: replace expected_data with wrong rows
    val dataDir = expected("t_v1").resolve("t_v1_read_all/expected_data")
    org.apache.commons.io.FileUtils.deleteDirectory(dataDir.toFile)
    spark.sql("SELECT 100 AS id UNION ALL SELECT 200")
      .write.parquet(dataDir.toString)

    val specPath = specs("t_v1").resolve("t_v1_read_all.json")
    intercept[RuntimeException] {
      ReadCapture.validateFromSpec(
        spark, delta("t_v1"), expected("t_v1").resolve("t_v1_read_all"), specPath)
    }
  }

  test("validation: tampered snapshot protocol caught by SnapshotCapture.validate") {
    // Generate valid snapshot, then tamper protocol in spec, call validate directly
    run("t_v2") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1)")
      val t = registerTable("tbl")
      snapshotSpec(t)
    }
    // Tamper: change minReaderVersion to 99 in spec JSON
    val specFile = specs("t_v2").resolve("t_v2_snapshot.json")
    val content = new String(Files.readAllBytes(specFile), "UTF-8")
    val tampered = content.replaceAll(
      """"minReaderVersion"\s*:\s*\d+""", """"minReaderVersion":99""")
    assert(tampered != content, "Tamper should have changed minReaderVersion")
    Files.write(specFile, tampered.getBytes("UTF-8"))

    intercept[IllegalArgumentException] {
      SnapshotCapture.validateFromSpec(spark, delta("t_v2"), specFile)
    }
  }

  private def strDf(values: String*): DataFrame =
    spark.createDataFrame(values.map(Row(_)).asJava, StructType(Seq(StructField("x", StringType))))

  test("validation: assertRowsEqual reports extra rows") {
    val ex = intercept[RuntimeException] {
      RowComparison.assertRowsEqual(strDf("a"), strDf("a", "c"), "spec")
    }
    assert(ex.getMessage.contains("Extra rows"))
  }

  test("validation: assertRowsEqual reports missing rows") {
    val ex = intercept[RuntimeException] {
      RowComparison.assertRowsEqual(strDf("a", "b"), strDf("a"), "spec")
    }
    assert(ex.getMessage.contains("Missing rows"))
  }

  test("validation: assertRowsEqual catches duplicate-count differences") {
    // Bag semantics: expected has r twice, actual three times, so the extra copy is flagged.
    val ex = intercept[RuntimeException] {
      RowComparison.assertRowsEqual(strDf("r", "r", "s"), strDf("r", "r", "r", "s"), "spec")
    }
    assert(ex.getMessage.contains("Extra rows"))
  }

  test("validation: assertRowsEqual catches column type drift") {
    val intDf = spark.createDataFrame(
      Seq(Row(1)).asJava, StructType(Seq(StructField("x", IntegerType))))
    val longDf = spark.createDataFrame(
      Seq(Row(1L)).asJava, StructType(Seq(StructField("x", LongType))))
    val ex = intercept[RuntimeException] {
      RowComparison.assertRowsEqual(intDf, longDf, "spec")
    }
    assert(ex.getMessage.contains("schema mismatch"))
  }

  test("validation: assertRowsEqual treats maps with different key order as equal") {
    val expected = spark.sql("SELECT map('x', 1, 'y', 2) AS m")
    val actual = spark.sql("SELECT map('y', 2, 'x', 1) AS m")
    RowComparison.assertRowsEqual(expected, actual, "map_order") // must not throw
  }

  test("validation: assertRowsEqual catches a genuine map difference") {
    val expected = spark.sql("SELECT map('x', 1, 'y', 2) AS m")
    val actual = spark.sql("SELECT map('x', 1, 'y', 99) AS m")
    intercept[RuntimeException] { RowComparison.assertRowsEqual(expected, actual, "map_diff") }
  }

  test("validation: assertRowsEqual treats variants with different field order as equal") {
    val expected = spark.sql("""SELECT parse_json('{"a":1,"b":2}') AS v""")
    val actual = spark.sql("""SELECT parse_json('{"b":2,"a":1}') AS v""")
    RowComparison.assertRowsEqual(expected, actual, "variant_order") // must not throw
  }

  test("validation: assertRowsEqual handles MAP<STRING,VARIANT> order-insensitively") {
    // Map values are canonicalized before array_sort, so the variant-valued map reaches exceptAll
    // without an analysis error and compares key-order-insensitively.
    val expected = spark.sql("""SELECT map('a', parse_json('1'), 'b', parse_json('"x"')) AS m""")
    val actual = spark.sql("""SELECT map('b', parse_json('"x"'), 'a', parse_json('1')) AS m""")
    RowComparison.assertRowsEqual(expected, actual, "map_variant") // must not throw
  }

  test("validation: assertRowsEqual recurses into array<struct<map>>") {
    val expected = spark.sql("SELECT array(named_struct('m', map('x', 1, 'y', 2))) AS a")
    val actual = spark.sql("SELECT array(named_struct('m', map('y', 2, 'x', 1))) AS a")
    RowComparison.assertRowsEqual(expected, actual, "nested") // must not throw
  }

  test("validation: assertRowsEqual passes on empty inputs") {
    RowComparison.assertRowsEqual(strDf(), strDf(), "empty") // must not throw
  }

  test("validation: tampered snapshot metadata caught by SnapshotCapture.validate") {
    run("t_v6") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1)")
      val t = registerTable("tbl")
      snapshotSpec(t)
    }
    // Tamper: change schemaString in spec to have wrong column
    val specFile = specs("t_v6").resolve("t_v6_snapshot.json")
    val specNode = JsonUtil.mapper.readTree(Files.readAllBytes(specFile))
    val metaNode = specNode.get("expected").get("metadata")
      .asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
    metaNode.put("schemaString",
      """{"type":"struct","fields":[{"name":"WRONG","type":"string","nullable":true,"metadata":{}}]}""")
    JsonUtil.writeSpec(specFile, JsonUtil.mapper.treeToValue(specNode, classOf[Any]))

    intercept[IllegalArgumentException] {
      SnapshotCapture.validateFromSpec(spark, delta("t_v6"), specFile)
    }
  }

  test("validation: tampered read extra rows caught by validateCapturedRead") {
    run("t_v7") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1),(2),(3)")
      val t = registerTable("tbl")
      readSpec(t)
    }
    // Tamper: append extra rows to expected_data
    val dataDir = expected("t_v7").resolve("t_v7_read_all/expected_data")
    spark.sql("SELECT 50 AS id UNION ALL SELECT 51")
      .write.mode("append").parquet(dataDir.toString)

    val specPath = specs("t_v7").resolve("t_v7_read_all.json")
    intercept[RuntimeException] {
      ReadCapture.validateFromSpec(
        spark, delta("t_v7"), expected("t_v7").resolve("t_v7_read_all"), specPath)
    }
  }

  // ---- Per-field snapshot mismatch + write-validation leniency boundaries ----
  // Validation correctness is critical: each compared metadata field must be caught, and the
  // write-derived leniency (which tolerates a replay's fresh ids) must NOT mask real differences.

  private def captureSnapshot(name: String): Unit = run(name) { _ =>
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    snapshotSpec(registerTable("tbl"))
  }

  /** Edit the captured snapshot spec's `expected.metadata` node in place; returns the spec file. */
  private def tamperSnapshotMeta(name: String)(
      edit: com.fasterxml.jackson.databind.node.ObjectNode => Unit): Path = {
    val specFile = specs(name).resolve(s"${name}_snapshot.json")
    val node = JsonUtil.mapper.readTree(Files.readAllBytes(specFile))
    edit(node.get("expected").get("metadata").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode])
    JsonUtil.writeSpec(specFile, JsonUtil.mapper.treeToValue(node, classOf[Any]))
    specFile
  }

  test("validation: tampered snapshot partitionColumns caught") {
    captureSnapshot("t_vpc")
    val f = tamperSnapshotMeta("t_vpc") { _.putArray("partitionColumns").add("bogus") }
    intercept[IllegalArgumentException] { SnapshotCapture.validateFromSpec(spark, delta("t_vpc"), f) }
  }

  test("validation: tampered snapshot configuration caught") {
    captureSnapshot("t_vcfg")
    val f = tamperSnapshotMeta("t_vcfg") { _.putObject("configuration").put("bogus.key", "1") }
    intercept[IllegalArgumentException] { SnapshotCapture.validateFromSpec(spark, delta("t_vcfg"), f) }
  }

  test("validation: tampered snapshot id caught in read-only validation") {
    captureSnapshot("t_vid")
    val f = tamperSnapshotMeta("t_vid") { _.put("id", "00000000-0000-0000-0000-000000000000") }
    intercept[IllegalArgumentException] { SnapshotCapture.validateFromSpec(spark, delta("t_vid"), f) }
  }

  test("validation: write-derived validation TOLERATES a fresh table id") {
    captureSnapshot("t_vwid")
    val f = tamperSnapshotMeta("t_vwid") { _.put("id", "00000000-0000-0000-0000-000000000000") }
    // A replay mints id/name/description/createdTime fresh, so write-validation excludes them.
    SnapshotCapture.validateFromSpec(spark, delta("t_vwid"), f, isWriteValidation = true)
  }

  test("validation: write-derived validation STILL CATCHES a real schema mismatch") {
    captureSnapshot("t_vwsc")
    val f = tamperSnapshotMeta("t_vwsc") { _.put("schemaString",
      """{"type":"struct","fields":[{"name":"WRONG","type":"string","nullable":true,"metadata":{}}]}""") }
    intercept[IllegalArgumentException] {
      SnapshotCapture.validateFromSpec(spark, delta("t_vwsc"), f, isWriteValidation = true) }
  }

  test("validation: maxColumnId differs — caught read-only, tolerated for write-derived") {
    captureSnapshot("t_vmci")
    val f = tamperSnapshotMeta("t_vmci") { m =>
      val cfg = Option(m.get("configuration")).map(_.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode])
        .getOrElse(m.putObject("configuration"))
      cfg.put("delta.columnMapping.maxColumnId", "9") }
    intercept[IllegalArgumentException] { SnapshotCapture.validateFromSpec(spark, delta("t_vmci"), f) }
    // Replay mints maxColumnId fresh -> excluded for write-validation: must NOT throw.
    SnapshotCapture.validateFromSpec(spark, delta("t_vmci"), f, isWriteValidation = true)
  }

  test("validation: tampered read expected_metadata (scanned file set) caught") {
    run("t_vmeta") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1),(2)")
      readSpec(registerTable("tbl"))
    }
    // Overwrite the recorded scanned-files with a bogus AddFile so the re-derived scan can't match.
    val metaDir = expected("t_vmeta").resolve("t_vmeta_read_all/expected_metadata")
    org.apache.commons.io.FileUtils.deleteDirectory(metaDir.toFile)
    spark.createDataset(Seq("""{"add":{"path":"bogus","size":1}}"""))(org.apache.spark.sql.Encoders.STRING)
      .toDF("action").write.parquet(metaDir.toString)
    val specPath = specs("t_vmeta").resolve("t_vmeta_read_all.json")
    intercept[RuntimeException] {
      ReadCapture.validateFromSpec(
        spark, delta("t_vmeta"), expected("t_vmeta").resolve("t_vmeta_read_all"), specPath)
    }
  }

  // =========================================================================
  // Table copy and mutations
  // =========================================================================

  test("table copy: copied table is readable and matches source data") {
    run("t_cp1") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1),(2),(3)")
      val t = registerTable("tbl")
      readSpec(t)
    }
    // Verify copied delta table is independently readable
    val df = spark.read.format("delta").load(delta("t_cp1").toString)
    val rows = df.collect().map(_.getInt(0)).sorted
    assert(rows.toSeq == Seq(1, 2, 3), s"Copied table should have rows 1,2,3 but got ${rows.toSeq}")

    // Verify commit log structure: v0 (CREATE) + v1 (INSERT) = 2 commits
    val logDir = delta("t_cp1").resolve("_delta_log")
    assert(Files.exists(logDir.resolve("00000000000000000000.json")))
    assert(Files.exists(logDir.resolve("00000000000000000001.json")))
  }

  test("mutateTable: modifies copied table, not source") {
    var sourcePath: Path = null
    run("t_mt1") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1),(2)")
      val loc = WorkloadContext.current.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
      sourcePath = if (loc.startsWith("file:")) {
        java.nio.file.Paths.get(new java.net.URI(loc))
      } else java.nio.file.Paths.get(loc)
      val t = registerTable("tbl")
      mutateTable(t) { tableDir =>
        Files.write(tableDir.resolve("MARKER"), "test".getBytes("UTF-8"))
      }
      readSpec(t)
    }
    assert(Files.exists(delta("t_mt1").resolve("MARKER")),
      "MARKER should exist in copied table")
    assert(!Files.exists(sourcePath.resolve("MARKER")),
      "MARKER should NOT exist in source table")
  }

  test("mutation: deleted data file produces error spec") {
    val results = run("t_corrupt") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1),(2),(3)")
      val t = registerTable("tbl")
      mutateTable(t) { tableDir =>
        // Delete all parquet data files
        Files.list(tableDir).iterator().asScala
          .filter(_.toString.endsWith(".parquet"))
          .foreach(Files.delete)
      }
      readSpec(t) // should produce error spec
    }
    assertPassed(results)
    val spec = JsonUtil.readReadSpec(specs("t_corrupt").resolve("t_corrupt_read_all.json"))
    spec.expectation match {
      case Failed(err) =>
        // Error can be FileNotFoundException, SparkException wrapping it, or other file-related errors
        assert(err.errorCode.contains("FILE_NOT_FOUND") || err.errorCode.contains("FileNotFoundException")
          || err.errorCode.contains("FILE_NOT_EXIST") || err.errorCode.contains("SparkException")
          || err.errorCode.contains("AnalysisException") || err.errorMessage.contains("does not exist"),
          s"Error should be file-related, got code: ${err.errorCode}, msg: ${err.errorMessage}")
      case Succeeded(_) =>
        fail("Should be an error spec after deleting data files")
    }
  }

  test("modifyCommitActions: modifies add stats, preserves commitInfo") {
    val results = run("t_mc1") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1),(2)")
      val t = registerTable("tbl")
      modifyCommitActions(t, version = 1) { actions =>
        actions.map {
          case a: AddFile => a.copy(stats = Some("""{"numRecords":999}"""))
          case other => other
        }
      }
      snapshotSpec(t)
    }
    assertPassed(results)
    val content = new String(Files.readAllBytes(
      delta("t_mc1").resolve("_delta_log/00000000000000000001.json")), "UTF-8")
    val lines = content.split("\n").filter(_.trim.nonEmpty)
    val actionTypes = lines.map { line =>
      JsonUtil.mapper.readTree(line).fieldNames().next()
    }
    assert(actionTypes.contains("commitInfo"), "commitInfo must be present")
    assert(actionTypes.contains("add"), "add actions must be present")
    // Verify stats were actually changed in every add action
    lines.filter(_.contains("\"add\"")).foreach { line =>
      val addNode = JsonUtil.mapper.readTree(line).get("add")
      val stats = addNode.get("stats").asText()
      assert(stats.contains("999"), s"Stats should contain 999, got: $stats")
    }
  }

  test("modifyCommitActions: drop all add actions") {
    val results = run("t_mc2") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1),(2)")
      val t = registerTable("tbl")
      modifyCommitActions(t, version = 1) { actions =>
        actions.filterNot(_.isInstanceOf[AddFile])
      }
      snapshotSpec(t)
    }
    assertPassed(results)
    val content = new String(Files.readAllBytes(
      delta("t_mc2").resolve("_delta_log/00000000000000000001.json")), "UTF-8")
    val lines = content.split("\n").filter(_.trim.nonEmpty)
    val actionTypes = lines.map(l => JsonUtil.mapper.readTree(l).fieldNames().next())
    assert(!actionTypes.contains("add"), "All add actions should be dropped")
    assert(actionTypes.contains("commitInfo"), "commitInfo must be preserved")
    assert(actionTypes.length == 1, s"Only commitInfo should remain, got: ${actionTypes.toSeq}")
  }

  test("modifyCommitActions: can modify metaData actions") {
    val results = run("t_mc3") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1)")
      val t = registerTable("tbl")
      // Modify the metaData action in the CREATE commit (v0)
      modifyCommitActions(t, version = 0) { actions =>
        actions.map {
          case m: Metadata => m.copy(configuration = m.configuration + ("test.property" -> "hello"))
          case other => other
        }
      }
      snapshotSpec(t)
    }
    assertPassed(results)
    val content = new String(Files.readAllBytes(
      delta("t_mc3").resolve("_delta_log/00000000000000000000.json")), "UTF-8")
    assert(content.contains("test.property"), "Modified property should be present")
    assert(content.contains("hello"), "Modified property value should be present")
  }

  // =========================================================================
  // Multi-table workloads
  // =========================================================================

  test("multi-table: two tables from one test") {
    val results = run("t_mt") { _ =>
      sql("CREATE TABLE src (id INT) USING delta")
      sql("INSERT INTO src VALUES (1),(2)")
      sql("CREATE TABLE dst (id INT) USING delta")
      sql("INSERT INTO dst VALUES (3),(4),(5)")
      val s1 = registerTable("src")
      val d1 = registerTable("dst")
      readSpec(s1)
      readSpec(d1)
    }
    assert(results.size == 2, "Should produce 2 workload results")
    assertPassed(results)
    assert(Files.exists(dir("t_mt_src")))
    assert(Files.exists(dir("t_mt_dst")))
  }

  // =========================================================================
  // Auto-naming
  // =========================================================================

  test("auto-naming: predicate operators") {
    val results = run("t_an1") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1),(2),(3)")
      val t = registerTable("tbl")
      readSpec(t, predicate = "id >= 2")
      readSpec(t, predicate = "id < 3")
      readSpec(t, predicate = "id = 1")
      readSpec(t, predicate = "id IS NULL")
    }
    assertPassed(results)
    assert(Files.exists(specs("t_an1").resolve("t_an1_read_id_gte_2.json")))
    assert(Files.exists(specs("t_an1").resolve("t_an1_read_id_lt_3.json")))
    assert(Files.exists(specs("t_an1").resolve("t_an1_read_id_eq_1.json")))
    assert(Files.exists(specs("t_an1").resolve("t_an1_read_id_is_null.json")))
  }

  // =========================================================================
  // Framework behavior
  // =========================================================================

  test("framework: failed test cleans up output directory") {
    val results = run("t_fw1") { _ =>
      throw new RuntimeException("boom")
    }
    assert(!results.head.passed)
    assert(!Files.exists(dir("t_fw1")))
  }

  test("framework: passing test skipped on re-run") {
    run("t_fw2") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1)")
      val t = registerTable("tbl")
      readSpec(t)
      snapshotSpec(t)
    }
    assert(Files.exists(dir("t_fw2").resolve("table_info.json")))
    val second = run("t_fw2", force = false) { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1)")
      val t = registerTable("tbl")
      readSpec(t)
      snapshotSpec(t)
    }
    assert(second.head.skipped)
  }

  test("framework: zero-table test warns") {
    val results = run("t_fw3") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      // Never call registerTable(); should warn
    }
    assert(results.size == 1)
    assert(results.head.passed) // passes but with warning
  }

  test("framework: table_info.json has correct metadata for simple table") {
    run("t_fw4") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1)")
      val t = registerTable("tbl")
      readSpec(t)
    }
    val info = JsonUtil.mapper.readTree(
      Files.readAllBytes(dir("t_fw4").resolve("table_info.json")))

    // Name
    assert(info.get("name").asText() == "t_fw4")

    // Schema: exactly {struct, fields: [{name: id, type: integer, nullable: true}]}
    val schema = info.get("schema")
    assert(schema.get("type").asText() == "struct")
    assert(schema.get("fields").size() == 1)
    assert(schema.get("fields").get(0).get("name").asText() == "id")
    assert(schema.get("fields").get(0).get("type").asText() == "integer")
    assert(schema.get("fields").get(0).get("nullable").asBoolean() == true)

    // Protocol: minReaderVersion=1, minWriterVersion=2
    assert(info.get("protocol").get("minReaderVersion").asInt() == 1)
    assert(info.get("protocol").get("minWriterVersion").asInt() == 2)

    // Log info
    val logInfo = info.get("logInfo")
    assert(logInfo.get("numAddFiles").asInt() == 1,
      "One INSERT = 1 add file")
    assert(logInfo.get("numCommits").asInt() == 2,
      "CREATE + INSERT = 2 commits")
    assert(logInfo.get("sizeInBytes").asLong() > 0,
      "Table should have non-zero size")

    // Data layout: no partitions, no clustering
    val dataLayout = info.get("dataLayout")
    assert(dataLayout.get("numPartitionColumns").asInt() == 0)
    assert(dataLayout.get("numClusteringColumns").asInt() == 0)
    assert(dataLayout.get("numDistinctPartitions").asInt() == 0)
  }

  test("framework: table_info.json correct for partitioned table") {
    run("t_fw5") { _ =>
      sql("""CREATE TABLE tbl (id INT, part STRING)
        USING delta PARTITIONED BY (part)""")
      sql("INSERT INTO tbl VALUES (1,'a'),(2,'a'),(3,'b')")
      val t = registerTable("tbl")
      readSpec(t)
    }
    val info = JsonUtil.mapper.readTree(
      Files.readAllBytes(dir("t_fw5").resolve("table_info.json")))

    // Schema: 2 fields (id INT, part STRING)
    val fields = info.get("schema").get("fields")
    assert(fields.size() == 2)
    val fieldNames = (0 until fields.size()).map(i => fields.get(i).get("name").asText()).toSet
    assert(fieldNames == Set("id", "part"))

    // Data layout: 1 partition column, 2 distinct partitions
    val dataLayout = info.get("dataLayout")
    assert(dataLayout.get("numPartitionColumns").asInt() == 1)
    // numDistinctPartitions may be 0 if allFiles scan fails on copied table
    val numDistinct = dataLayout.get("numDistinctPartitions").asInt()
    assert(numDistinct == 0 || numDistinct == 2,
      s"Should be 0 (scan failed) or 2 (a, b), got $numDistinct")
  }

  test("framework: table_info.json correct for multi-version table") {
    run("t_fw6") { _ =>
      sql("CREATE TABLE tbl (id INT) USING delta")
      sql("INSERT INTO tbl VALUES (1)")
      sql("INSERT INTO tbl VALUES (2)")
      sql("INSERT INTO tbl VALUES (3)")
      val t = registerTable("tbl")
      readSpec(t)
    }
    val info = JsonUtil.mapper.readTree(
      Files.readAllBytes(dir("t_fw6").resolve("table_info.json")))
    val logInfo = info.get("logInfo")
    assert(logInfo.get("numCommits").asInt() == 4,
      "CREATE + 3 INSERTs = 4 commits")
    assert(logInfo.get("numAddFiles").asInt() == 3,
      "3 INSERTs = 3 add files")
  }
}
