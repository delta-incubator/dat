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

package io.delta.workload.tables

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{ByteType, DataType, DoubleType, IntegerType, Metadata,
  StringType, StructType}

import io.delta.workload.{TableHandle, WorkloadContext, WorkloadTestSuite}
import io.delta.workload.engine.RowComparison
import io.delta.workload.json.JsonUtil
import io.delta.workload.log.CommitLog
import io.delta.workload.model.Succeeded

class SchemaEvolutionSuite extends WorkloadTestSuite("schema_evolution") {

  test("add_column") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'before')")
    sql("ALTER TABLE tbl ADD COLUMN (new_col DOUBLE)")
    sql("INSERT INTO tbl VALUES (2, 'after', 3.14)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    readSpec(t, columns = Some(Seq("id", "new_col")))
    readSpec(t, predicate = "new_col IS NOT NULL")
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("add_nested_field") {
    sql("CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    sql("ALTER TABLE tbl ADD COLUMNS (info.email STRING)")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25,'email','bob@test.com'))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "info.email IS NULL")
    readSpec(t, predicate = "info.email IS NOT NULL")
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("rename") {
    sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'before')")
    sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    sql("INSERT INTO tbl VALUES (2, 'after')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    readSpec(t, columns = Some(Seq("id", "new_name")))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("drop_column") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'alice','v1'),(2,'bob','v2')")
    sql("ALTER TABLE tbl DROP COLUMN value")
    sql("INSERT INTO tbl VALUES (3,'charlie')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 2)
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("multiple_renames") {
    sql("""CREATE TABLE tbl (id INT, a STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'first')")
    sql("ALTER TABLE tbl RENAME COLUMN a TO b")
    sql("INSERT INTO tbl VALUES (2,'second')")
    sql("ALTER TABLE tbl RENAME COLUMN b TO c")
    sql("INSERT INTO tbl VALUES (3,'third')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "c")))
    for (v <- 0L to 5L) snapshotSpec(t, version = v)
  }

  test("predicate_on_added") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob')")
    sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    sql("INSERT INTO tbl VALUES (3,'charlie',95),(4,'diana',88)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "score > 90")
    readSpec(t, predicate = "score IS NULL")
    readSpec(t, predicate = "score IS NOT NULL")
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("add_col_pred_eq") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    sql("INSERT INTO tbl VALUES (4,'d',100),(5,'e',200),(6,'f',300)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "score = 200")
    readSpec(t, predicate = "score = 100 OR score IS NULL")
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("drop_col_pred") {
    sql("""CREATE TABLE tbl (id INT, name STRING, category STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'alice','A'),(2,'bob','B'),(3,'charlie','A')")
    sql("ALTER TABLE tbl DROP COLUMN category")
    sql("INSERT INTO tbl VALUES (4,'diana'),(5,'eve')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id > 3")
    readSpec(t, predicate = "name = 'alice'")
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("rename_pred") {
    sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie')")
    sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    sql("INSERT INTO tbl VALUES (4,'diana')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "new_name = 'alice'")
    readSpec(t, predicate = "new_name = 'diana'")
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("rename_partition") {
    sql("""CREATE TABLE tbl (id INT, category STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'A'),(2,'B'),(3,'A'),(4,'C')")
    sql("ALTER TABLE tbl RENAME COLUMN category TO cat")
    sql("INSERT INTO tbl VALUES (5,'A'),(6,'C')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "cat = 'A'")
    readSpec(t, predicate = "cat = 'C'")
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("drop_readd_same_name") {
    sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'hello'),(2,'world')")
    sql("ALTER TABLE tbl DROP COLUMN x")
    sql("ALTER TABLE tbl ADD COLUMN (x INT)")
    sql("INSERT INTO tbl VALUES (3,100),(4,200)")
    val t = registerTable("tbl")
    readSpec(t)
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("readd_pred") {
    sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'old1'),(2,'old2')")
    sql("ALTER TABLE tbl DROP COLUMN x")
    sql("ALTER TABLE tbl ADD COLUMN (x INT)")
    sql("INSERT INTO tbl VALUES (3,100),(4,200)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "x = 200")
    readSpec(t, predicate = "x IS NULL")
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("dv_pred_null") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie'),(4,'diana')")
    sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    sql("INSERT INTO tbl VALUES (5,'eve',90),(6,'frank',80)")
    sql("DELETE FROM tbl WHERE id IN (2, 5)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "score IS NULL")
    readSpec(t, predicate = "score IS NOT NULL")
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("rename_read_v1") {
    sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'before_rename')")
    sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    sql("INSERT INTO tbl VALUES (2,'after_rename')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("nested_field_pred") {
    sql("CREATE TABLE tbl (id INT, info STRUCT<name: STRING>) USING delta")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice'))")
    sql("ALTER TABLE tbl ADD COLUMNS (info.email STRING)")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','email','bob@x.com'))")
    sql("INSERT INTO tbl VALUES (3, named_struct('name','charlie','email','charlie@y.com'))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "info.email IS NULL")
    readSpec(t, predicate = "info.email IS NOT NULL")
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("proj_at_old_version") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'one'),(2,'two')")
    sql("ALTER TABLE tbl ADD COLUMNS (extra INT)")
    sql("INSERT INTO tbl VALUES (3,'three',300)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1, columns = Some(Seq("id", "value")))
    readSpec(t, columns = Some(Seq("id", "extra")))
    for (v <- 0L to 3L) snapshotSpec(t, version = v)
  }

  test("type_coercion_insert") {
    sql("CREATE TABLE tbl (id LONG, value LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1, 100)")
    // Insert int values into long columns (implicit coercion)
    sql("INSERT INTO tbl SELECT CAST(2 AS INT), CAST(200 AS INT)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value > 150")
    for (v <- 0L to 2L) snapshotSpec(t, version = v)
  }

  test("merge_with_evolution") {
    sql("""CREATE TABLE target (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'false')""")
    sql("INSERT INTO target VALUES (1,'alice'),(2,'bob')")
    sql("CREATE TABLE src (id INT, name STRING, score INT) USING delta")
    sql("INSERT INTO src VALUES (2,'bob_updated',95),(3,'charlie',88)")
    sql("""MERGE INTO target t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("target")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "score")))
    for (v <- 0L to 2L) snapshotSpec(t, version = v)
  }

  test("add_col_pred_null") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'alice')")
    sql("INSERT INTO tbl VALUES (2, 'bob')")
    sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    sql("INSERT INTO tbl VALUES (3, 'charlie', 95)")
    sql("INSERT INTO tbl VALUES (4, 'diana', 88)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "score > 90")
    readSpec(t, predicate = "score IS NOT NULL")
    readSpec(t, predicate = "score IS NULL")
    snapshotSpec(t)
  }

  test("add_col_read_v1") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'alice')")
    sql("INSERT INTO tbl VALUES (2, 'bob')")
    sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    sql("INSERT INTO tbl VALUES (3, 'charlie', 95)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    snapshotSpec(t)
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("add_column_with_default") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    sql("INSERT INTO tbl VALUES (2)")
    sql("ALTER TABLE tbl ADD COLUMNS (status STRING)")
    sql("INSERT INTO tbl VALUES (3, 'active')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "status")))
    snapshotSpec(t)
  }

  test("drop_col_read_v1") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'alice', 10)")
    sql("INSERT INTO tbl VALUES (2, 'bob', 20)")
    sql("ALTER TABLE tbl DROP COLUMN value")
    sql("INSERT INTO tbl VALUES (3, 'charlie')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    snapshotSpec(t)
    for (v <- 0L to 4L) snapshotSpec(t, version = v)
  }

  test("nested_field_project") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25))")
    sql("ALTER TABLE tbl ADD COLUMNS (info.email STRING)")
    sql("INSERT INTO tbl VALUES (3, named_struct('name','charlie','age',35,'email','c@test.com'))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id")))
    readSpec(t, columns = Some(Seq("id", "info")))
    snapshotSpec(t)
  }

  test("pred_on_added_col") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'alice')")
    sql("INSERT INTO tbl VALUES (2, 'bob')")
    sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    sql("INSERT INTO tbl VALUES (3, 'charlie', 95)")
    sql("INSERT INTO tbl VALUES (4, 'diana', 88)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "score IS NOT NULL")
    snapshotSpec(t)
  }

  test("rename_chain") {
    sql("""CREATE TABLE tbl (id INT, a STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'first')")
    sql("ALTER TABLE tbl RENAME COLUMN a TO b")
    sql("INSERT INTO tbl VALUES (2, 'second')")
    sql("ALTER TABLE tbl RENAME COLUMN b TO c")
    sql("INSERT INTO tbl VALUES (3, 'third')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "c")))
    snapshotSpec(t)
  }

  test("rename_column") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'alice')")
    sql("INSERT INTO tbl VALUES (2, 'bob')")
    sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    sql("INSERT INTO tbl VALUES (3, 'charlie')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "full_name")))
    snapshotSpec(t)
  }

  test("rename_part_pred") {
    sql("""CREATE TABLE tbl (id INT, category STRING, value INT) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'A', 100)")
    sql("INSERT INTO tbl VALUES (2, 'B', 200)")
    sql("INSERT INTO tbl VALUES (3, 'A', 300)")
    sql("ALTER TABLE tbl RENAME COLUMN category TO cat")
    sql("INSERT INTO tbl VALUES (4, 'C', 400)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "cat = 'A'")
    readSpec(t, predicate = "cat = 'C'")
    snapshotSpec(t)
    for (v <- 0L to 5L) snapshotSpec(t, version = v)
  }

  test("rename_partition_column") {
    sql("""CREATE TABLE tbl (id INT, category STRING, value INT) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'A', 100)")
    sql("INSERT INTO tbl VALUES (2, 'B', 200)")
    sql("ALTER TABLE tbl RENAME COLUMN category TO group_name")
    sql("INSERT INTO tbl VALUES (3, 'C', 300)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "group_name", "value")))
    snapshotSpec(t)
  }

  test("add_nested_struct_field") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25))")
    sql("ALTER TABLE tbl ADD COLUMNS (info.email STRING)")
    sql("INSERT INTO tbl VALUES (3, named_struct('name','charlie','age',35,'email','c@test.com'))")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("drop_and_readd_same_name") {
    sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'hello')")
    sql("ALTER TABLE tbl DROP COLUMN x")
    sql("ALTER TABLE tbl ADD COLUMN (x INT)")
    sql("INSERT INTO tbl VALUES (2, 42)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("add_top_level_column") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.enableTypeWidening' = 'false')""")
    sql("INSERT INTO tbl VALUES (1, 'alice')")
    sql("INSERT INTO tbl VALUES (2, 'bob')")
    sql("ALTER TABLE tbl ADD COLUMNS (age INT)")
    sql("INSERT INTO tbl VALUES (3, 'charlie', 30)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, columns = Some(Seq("id", "name", "age")))
    snapshotSpec(t)
  }

  // CREATE OR REPLACE with same-name columns of a different type, changed partition and
  // file sets, and fresh column-mapping identities.
  // Shared by the stats-column and partition-column variants of the same replacement scenario.
  private def replaceSameName(
      caseName: String,
      partitioned: Boolean,
      fromType: DataType,
      toType: DataType,
      seedValues: String,
      replacementValues: String,
      beforeRows: Seq[Row],
      afterRows: Seq[Row],
      matchingPredicate: String,
      missingPredicate: String): Unit = {
    val partitionClause = if (partitioned) "PARTITIONED BY (c)" else ""
    val beforeSchema = new StructType().add("id", IntegerType)
      .add("c", fromType).add("payload", StringType)
    val afterSchema = new StructType().add("id", IntegerType)
      .add("c", toType).add("payload", StringType)

    sql(s"CREATE TABLE tbl (${beforeSchema.toDDL}) USING delta $partitionClause")
    sql(s"INSERT INTO tbl VALUES $seedValues")
    // This is a REPLACE TABLE commit, rather than DROP/ADD COLUMN or an append with evolution.
    sql(s"CREATE OR REPLACE TABLE tbl (${afterSchema.toDDL}) USING delta $partitionClause")
    sql(s"INSERT INTO tbl VALUES $replacementValues")
    val t = registerTable("tbl")
    val table = io.delta.workload.WorkloadContext.current.getTableSpec(t)

    // Literal rows form an oracle independent of reading the generated Delta table.
    def frame(rows: Seq[Row], schema: StructType): DataFrame =
      spark.createDataFrame(rows.asJava, schema)

    def checkRead(
        name: String,
        rows: Seq[Row],
        schema: StructType,
        version: java.lang.Long = null,
        predicate: String = null,
        columns: Option[Seq[String]] = None): Unit = {
      val oracle = frame(rows, schema)
      val projectedOracle = columns.map(cs => oracle.selectExpr(cs: _*)).getOrElse(oracle)
      readSpec(t, name = Some(name), version = version, predicate = predicate,
        columns = columns).assert { spec =>
        val workloadName = table.outputName
        spec.expectation match {
          case Succeeded(result) => assert(result.rowCount == rows.size.toLong)
          case other => fail(s"$workloadName/$name must succeed: $other")
        }
        val expectedPath = outputDir.resolve(workloadName).resolve("expected")
          .resolve(s"${workloadName}_$name").resolve("expected_data")
        val captured = spark.read.parquet(expectedPath.toString)
        assert(captured.schema == projectedOracle.schema)
        RowComparison.assertRowsEqual(
          projectedOracle, captured, s"$workloadName/$name literal oracle")
        println(s"Independent row oracle passed: $workloadName/$name (${rows.size} rows)")
      }
    }

    checkRead("read_all", afterRows, afterSchema)
    checkRead("read_before", beforeRows, beforeSchema, version = 1L)
    checkRead("read_replaced_empty", Seq.empty, afterSchema, version = 2L)
    checkRead("read_match", afterRows.take(2), afterSchema, predicate = matchingPredicate)
    checkRead("read_missing", Seq.empty, afterSchema, predicate = missingPredicate)
    checkRead("read_null", afterRows.slice(3, 4), afterSchema, predicate = "c IS NULL")
    checkRead("read_non_null", afterRows.take(3) ++ afterRows.drop(4), afterSchema,
      predicate = "c IS NOT NULL")
    checkRead("read_projection", afterRows, afterSchema, columns = Some(Seq("c", "payload")))
    checkRead("read_before_projection", beforeRows, beforeSchema, version = 1L,
      columns = Some(Seq("c", "payload")))
    checkRead("read_before_null", beforeRows.takeRight(1), beforeSchema, version = 1L,
      predicate = "c IS NULL")

    for (v <- 0L to 3L) {
      val expectedSchema = if (v < 2L) beforeSchema else afterSchema
      snapshotSpec(t, version = v).assert { spec =>
        spec.expectation match {
          case Succeeded(result) =>
            assert(DataType.fromJson(result.metadata.schemaString) == expectedSchema)
            assert(result.metadata.partitionColumns == (if (partitioned) Seq("c") else Seq.empty))
          case other => fail(s"${table.outputName} snapshot v$v must succeed: $other")
        }
      }
    }
  }

  test("string_to_int_stats") {
    replaceSameName(
      "string_to_int_stats", partitioned = false, StringType, IntegerType,
      seedValues = """(1, 'not-an-int', 'old'), (1, 'not-an-int', 'old'), """ +
        """(2, '007', 'old-padded'), (3, NULL, 'old-null')""",
      replacementValues = """(101, 7, 'new-seven'), (101, 7, 'new-seven'), """ +
        """(102, -5, 'new-negative'), (103, NULL, 'new-null'), (104, 0, 'new-zero')""",
      beforeRows = Seq(
        Row(1, "not-an-int", "old"),
        Row(1, "not-an-int", "old"),
        Row(2, "007", "old-padded"),
        Row(3, null, "old-null")),
      afterRows = Seq(
        Row(101, 7, "new-seven"),
        Row(101, 7, "new-seven"),
        Row(102, -5, "new-negative"),
        Row(103, null, "new-null"),
        Row(104, 0, "new-zero")),
      matchingPredicate = "c = 7", missingPredicate = "c = 999")
  }

  test("int_to_string_stats") {
    replaceSameName(
      "int_to_string_stats", partitioned = false, IntegerType, StringType,
      seedValues = """(1, 7, 'old'), (1, 7, 'old'), (2, -5, 'old-negative'), """ +
        """(3, NULL, 'old-null')""",
      replacementValues = """(101, 'seven', 'new-word'), (101, 'seven', 'new-word'), """ +
        """(102, '007', 'new-padded'), (103, NULL, 'new-null'), (104, '', 'new-empty')""",
      beforeRows = Seq(
        Row(1, 7, "old"),
        Row(1, 7, "old"),
        Row(2, -5, "old-negative"),
        Row(3, null, "old-null")),
      afterRows = Seq(
        Row(101, "seven", "new-word"),
        Row(101, "seven", "new-word"),
        Row(102, "007", "new-padded"),
        Row(103, null, "new-null"),
        Row(104, "", "new-empty")),
      matchingPredicate = "c = 'seven'", missingPredicate = "c = 'missing'")
  }

  test("byte_to_string_stats") {
    replaceSameName(
      "byte_to_string_stats", partitioned = false, ByteType, StringType,
      seedValues = """(1, CAST(-128 AS BYTE), 'old-min'), (1, CAST(-128 AS BYTE), 'old-min'), """ +
        """(2, CAST(127 AS BYTE), 'old-max'), (3, CAST(NULL AS BYTE), 'old-null')""",
      replacementValues = """(101, 'byte-min', 'new-word'), (101, 'byte-min', 'new-word'), """ +
        """(102, '127', 'new-numeric'), (103, NULL, 'new-null'), (104, '', 'new-empty')""",
      beforeRows = Seq(
        Row(1, -128.toByte, "old-min"),
        Row(1, -128.toByte, "old-min"),
        Row(2, 127.toByte, "old-max"),
        Row(3, null, "old-null")),
      afterRows = Seq(
        Row(101, "byte-min", "new-word"),
        Row(101, "byte-min", "new-word"),
        Row(102, "127", "new-numeric"),
        Row(103, null, "new-null"),
        Row(104, "", "new-empty")),
      matchingPredicate = "c = 'byte-min'", missingPredicate = "c = 'missing'")
  }

  test("string_to_int_partition") {
    replaceSameName(
      "string_to_int_partition", partitioned = true, StringType, IntegerType,
      seedValues = """(1, 'not-an-int', 'old'), (1, 'not-an-int', 'old'), """ +
        """(2, '007', 'old-padded'), (3, NULL, 'old-null')""",
      replacementValues = """(101, 7, 'new-seven'), (101, 7, 'new-seven'), """ +
        """(102, -5, 'new-negative'), (103, NULL, 'new-null'), (104, 0, 'new-zero')""",
      beforeRows = Seq(
        Row(1, "not-an-int", "old"),
        Row(1, "not-an-int", "old"),
        Row(2, "007", "old-padded"),
        Row(3, null, "old-null")),
      afterRows = Seq(
        Row(101, 7, "new-seven"),
        Row(101, 7, "new-seven"),
        Row(102, -5, "new-negative"),
        Row(103, null, "new-null"),
        Row(104, 0, "new-zero")),
      matchingPredicate = "c = 7", missingPredicate = "c = 999")
  }

  test("int_to_string_partition") {
    replaceSameName(
      "int_to_string_partition", partitioned = true, IntegerType, StringType,
      seedValues = """(1, 7, 'old'), (1, 7, 'old'), (2, -5, 'old-negative'), """ +
        """(3, NULL, 'old-null')""",
      replacementValues = """(101, 'seven', 'new-word'), (101, 'seven', 'new-word'), """ +
        """(102, '007', 'new-padded'), (103, NULL, 'new-null'), (104, 'zero', 'new-zero')""",
      beforeRows = Seq(
        Row(1, 7, "old"),
        Row(1, 7, "old"),
        Row(2, -5, "old-negative"),
        Row(3, null, "old-null")),
      afterRows = Seq(
        Row(101, "seven", "new-word"),
        Row(101, "seven", "new-word"),
        Row(102, "007", "new-padded"),
        Row(103, null, "new-null"),
        Row(104, "zero", "new-zero")),
      matchingPredicate = "c = 'seven'", missingPredicate = "c = 'missing'")
  }

  test("byte_to_string_partition") {
    replaceSameName(
      "byte_to_string_partition", partitioned = true, ByteType, StringType,
      seedValues = """(1, CAST(-128 AS BYTE), 'old-min'), (1, CAST(-128 AS BYTE), 'old-min'), """ +
        """(2, CAST(127 AS BYTE), 'old-max'), (3, CAST(NULL AS BYTE), 'old-null')""",
      replacementValues = """(101, 'byte-min', 'new-word'), (101, 'byte-min', 'new-word'), """ +
        """(102, '127', 'new-numeric'), (103, NULL, 'new-null'), (104, 'zero', 'new-zero')""",
      beforeRows = Seq(
        Row(1, -128.toByte, "old-min"),
        Row(1, -128.toByte, "old-min"),
        Row(2, 127.toByte, "old-max"),
        Row(3, null, "old-null")),
      afterRows = Seq(
        Row(101, "byte-min", "new-word"),
        Row(101, "byte-min", "new-word"),
        Row(102, "127", "new-numeric"),
        Row(103, null, "new-null"),
        Row(104, "zero", "new-zero")),
      matchingPredicate = "c = 'byte-min'", missingPredicate = "c = 'missing'")
  }

  // Changed partition/file sets and fresh mapped identities share the same replacement oracle.
  private def replacementRead(
      t: TableHandle,
      name: String,
      rows: Seq[Row],
      schema: StructType,
      version: java.lang.Long = null,
      predicate: String = null,
      columns: Option[Seq[String]] = None): Unit = {
    val table = io.delta.workload.WorkloadContext.current.getTableSpec(t)
    val ref = readSpec(t, name = Some(name), version = version, predicate = predicate,
      columns = columns)
    ref.assert { spec =>
      val workloadName = table.outputName
      spec.expectation match {
        case Succeeded(result) => assert(result.rowCount == rows.size.toLong)
        case other => fail(s"$workloadName/$name must succeed: $other")
      }
      val literal = spark.createDataFrame(rows.asJava, schema)
      val oracle = columns.map(cs => literal.selectExpr(cs: _*)).getOrElse(literal)
      val path = outputDir.resolve(workloadName).resolve("expected")
        .resolve(s"${workloadName}_${name}").resolve("expected_data")
      val captured = spark.read.parquet(path.toString)
      // RowComparison checks exact names/types and duplicate-sensitive row bags.
      RowComparison.assertRowsEqual(oracle, captured, s"$workloadName/$name literal oracle")
      assert(captured.schema.fields.forall(_.nullable))
      println(s"Independent row oracle passed: $workloadName/$name (${rows.size} rows)")
    }
  }

  private def replacementLayout(
      caseName: String,
      layout: String,
      mappingMode: Option[String] = None): Unit = {
    val mapped = mappingMode.isDefined
    val parsed = layout == "checkpoint_parsed"
    val checkpointed = layout != "commits"
    val beforeNames = if (mapped) Seq("id", "label", "score", "region")
      else Seq("id", "old_label", "old_score", "old_region")
    val afterNames = Seq("id", "label", "score", "region")
    def logicalSchema(names: Seq[String]): StructType = new StructType()
      .add(names(0), IntegerType).add(names(1), StringType)
      .add(names(2), DoubleType).add(names(3), StringType)
    val beforeSchema = logicalSchema(beforeNames)
    val afterSchema = logicalSchema(afterNames)
    val beforeRows = Seq(
      Row(1, "old", 1000.0, "old-east"), Row(1, "old", 1000.0, "old-east"),
      Row(2, "old-negative", -1000.0, "old-west"), Row(3, null, null, null))
    val afterRows = Seq(
      Row(101, "new-a", 7.5, "east"), Row(101, "new-a", 7.5, "east"),
      Row(102, "new-b", -2.5, "west"), Row(103, null, null, null),
      Row(104, "new-zero", 0.0, "east"))
    val properties = Map(
      "delta.checkpointInterval" -> "1000",
      "delta.checkpoint.writeStatsAsJson" -> (!parsed).toString,
      "delta.checkpoint.writeStatsAsStruct" -> parsed.toString) ++
      mappingMode.map("delta.columnMapping.mode" -> _).toMap
    val propertySql = properties.toSeq.sortBy(_._1)
      .map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")
    sql(s"""CREATE TABLE tbl (${beforeSchema.toDDL}) USING delta
      PARTITIONED BY (${beforeNames.last}) TBLPROPERTIES ($propertySql)""")
    sql("""INSERT INTO tbl VALUES
      (1, 'old', 1000D, 'old-east'), (1, 'old', 1000D, 'old-east'),
      (2, 'old-negative', -1000D, 'old-west'), (3, NULL, NULL, NULL)""")

    // The setting exists in OSS Delta 4.1.0 as well as Runtime. Restore after the DDL;
    // it controls authoring only, and is not needed while queued read specs are captured.
    val reuseKey = "spark.databricks.delta.columnMapping.reuseColumnMetadataDuringReplace"
    val oldReuse = spark.conf.getOption(reuseKey)
    try {
      if (mapped) spark.conf.set(reuseKey, "false")
      sql(s"""CREATE OR REPLACE TABLE tbl (${afterSchema.toDDL}) USING delta
        PARTITIONED BY (region) TBLPROPERTIES ($propertySql)""")
    } finally {
      if (mapped) oldReuse match {
        case Some(value) => spark.conf.set(reuseKey, value)
        case None => spark.conf.unset(reuseKey)
      }
    }
    sql("""INSERT INTO tbl VALUES
      (101, 'new-a', 7.5D, 'east'), (101, 'new-a', 7.5D, 'east'),
      (102, 'new-b', -2.5D, 'west'), (103, NULL, NULL, NULL),
      (104, 'new-zero', 0D, 'east')""")
    if (checkpointed) forceCheckpoint("tbl")
    val t = registerTable("tbl")
    val rawBefore = CommitLog.readRaw(t.sourcePath, 0L).map(JsonUtil.mapper.readTree)
      .find(_.has("metaData")).get.get("metaData")
    val rawAfter = CommitLog.readRaw(t.sourcePath, 2L).map(JsonUtil.mapper.readTree)
      .find(_.has("metaData")).get.get("metaData")
    val mappedBefore = DataType.fromJson(rawBefore.get("schemaString").asText)
      .asInstanceOf[StructType]
    val mappedAfter = DataType.fromJson(rawAfter.get("schemaString").asText)
      .asInstanceOf[StructType]
    def withoutMapping(schema: StructType): StructType =
      StructType(schema.fields.map(_.copy(metadata = Metadata.empty)))
    assert(withoutMapping(mappedBefore) == beforeSchema)
    assert(withoutMapping(mappedAfter) == afterSchema)
    if (mapped) {
      val idKey = "delta.columnMapping.id"
      val physicalKey = "delta.columnMapping.physicalName"
      assert(mappedBefore.fields.map(_.metadata.getLong(idKey)).toSeq == Seq(1L, 2L, 3L, 4L))
      assert(mappedAfter.fields.map(_.metadata.getLong(idKey)).toSeq == Seq(5L, 6L, 7L, 8L))
      val oldPhysical = mappedBefore.fields.map(_.metadata.getString(physicalKey)).toSet
      val newPhysical = mappedAfter.fields.map(_.metadata.getString(physicalKey)).toSet
      assert(oldPhysical.size == 4 && newPhysical.size == 4)
      assert(oldPhysical.intersect(newPhysical).isEmpty)
      assert(newPhysical.intersect(afterNames.toSet).isEmpty)
    }
    val physicalNames = mappedAfter.fields.map { field =>
      if (mapped) field.metadata.getString("delta.columnMapping.physicalName") else field.name
    }.toSeq
    val statsNames = physicalNames.init.toSet
    val partitionName = physicalNames.last
    // Inspect new commit actions directly too, including the commit-only mapping variants.
    val newAdds = CommitLog.readRaw(t.sourcePath, 3L).map(JsonUtil.mapper.readTree)
      .filter(_.has("add")).map(_.get("add"))
    assert(newAdds.nonEmpty)
    for (add <- newAdds) {
      assert(add.get("partitionValues").fieldNames().asScala.toSet == Set(partitionName))
      val stats = JsonUtil.mapper.readTree(add.get("stats").asText)
      assert(stats.get("nullCount").fieldNames().asScala.toSet == statsNames)
      for (kind <- Seq("minValues", "maxValues")) {
        assert(stats.get(kind).fieldNames().asScala.toSet.subsetOf(statsNames))
      }
    }

    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      // Retain exact pre-truncation actions in command evidence, including operation/removes.
      for (v <- 0L to 3L; line <- CommitLog.readRaw(tableDir, v)) {
        println(s"Replacement source action: $caseName v$v $line")
      }
      val stream = Files.list(logDir)
      val files = try stream.iterator().asScala.toSeq finally stream.close()
      if (checkpointed) {
        val checkpoints = files.filter(_.getFileName.toString.endsWith(".checkpoint.parquet"))
        assert(checkpoints.size == 1)
        assert(checkpoints.head.getFileName.toString == "00000000000000000003.checkpoint.parquet")
        val checkpoint = spark.read.parquet(checkpoints.head.toString)
        val addSchema = checkpoint.schema("add").dataType.asInstanceOf[StructType]
        assert(addSchema.fieldNames.contains("stats") == !parsed)
        assert(addSchema.fieldNames.contains("stats_parsed") == parsed)
        assert(addSchema.fieldNames.contains("partitionValues_parsed") == parsed)
        if (parsed) {
          val partitions = addSchema("partitionValues_parsed").dataType.asInstanceOf[StructType]
          assert(partitions.fieldNames.toSeq == Seq(partitionName))
          assert(partitions.fields.head.dataType == StringType)
          val stats = addSchema("stats_parsed").dataType.asInstanceOf[StructType]
          for (kind <- Seq("minValues", "maxValues", "nullCount")) {
            assert(stats(kind).dataType.asInstanceOf[StructType].fieldNames.toSet == statsNames)
          }
        }
        val adds = checkpoint.filter("add IS NOT NULL").select("add").toJSON.collect()
        assert(adds.nonEmpty)
        for (json <- adds) {
          val add = JsonUtil.mapper.readTree(json).get("add")
          assert(add.get("partitionValues").fieldNames().asScala.toSet == Set(partitionName))
          val stats = if (parsed) add.get("stats_parsed")
            else JsonUtil.mapper.readTree(add.get("stats").asText)
          assert(stats.get("nullCount").fieldNames().asScala.toSet == statsNames)
          for (kind <- Seq("minValues", "maxValues")) {
            assert(stats.get(kind).fieldNames().asScala.toSet.subsetOf(statsNames))
          }
        }
        // Keep only the checkpoint-era JSON for version discovery. Earlier metadata and file
        // state must come from the checkpoint; remove protocol CRC shortcuts as well.
        files.filter { path =>
          val name = path.getFileName.toString
          name.endsWith(".crc") ||
            (name.matches("[0-9]{20}\\.json") && name != "00000000000000000003.json")
        }.foreach(Files.delete)
      } else {
        assert(!files.exists(_.getFileName.toString.contains("checkpoint")))
        files.filter(_.getFileName.toString.endsWith(".crc")).foreach(Files.delete)
      }
      println(s"Replacement layout oracle passed: $caseName ($layout)")
    }

    replacementRead(t, "read_all", afterRows, afterSchema)
    replacementRead(t, "read_projection", afterRows, afterSchema,
      columns = Some(Seq("region", "score", "label")))
    replacementRead(t, "read_partition", afterRows.take(2) ++ afterRows.drop(4),
      afterSchema, predicate = "region = 'east'")
    replacementRead(t, "read_stats", afterRows.take(2), afterSchema,
      predicate = "score > 7D")
    replacementRead(t, "read_missing", Seq.empty, afterSchema,
      predicate = "region = 'missing' OR score > 999D")
    replacementRead(t, "read_null", afterRows.slice(3, 4), afterSchema,
      predicate = "region IS NULL AND score IS NULL AND label IS NULL")
    replacementRead(t, "read_old_stats", Seq.empty, afterSchema,
      predicate = "score = 1000D OR label = 'old'")
    if (!checkpointed) {
      replacementRead(t, "read_before", beforeRows, beforeSchema, version = 1L)
      replacementRead(t, "read_replaced_empty", Seq.empty, afterSchema, version = 2L)
    }
    for (v <- (if (checkpointed) Seq(3L) else Seq(1L, 2L, 3L))) {
      val metadata = if (v < 2L) rawBefore else rawAfter
      val expectedProperties = properties ++ mappingMode.map { mode =>
        Map("delta.columnMapping.mode" -> mode,
          "delta.columnMapping.maxColumnId" -> (if (v < 2L) "4" else "8"))
      }.getOrElse(Map.empty[String, String])
      snapshotSpec(t, version = v).assert { spec =>
        spec.expectation match {
          case Succeeded(result) =>
            assert(result.metadata.schemaString == metadata.get("schemaString").asText)
            assert(result.metadata.partitionColumns ==
              Seq(if (v < 2L) beforeNames.last else "region"))
            assert(result.metadata.configuration == expectedProperties)
          case other => fail(s"$caseName snapshot v$v must succeed: $other")
        }
        println(s"Independent metadata oracle passed: $caseName v$v")
      }
    }
  }

  test("changed_sets_checkpoint_json") {
    replacementLayout("changed_sets_checkpoint_json", "checkpoint_json")
  }

  test("changed_sets_checkpoint_parsed") {
    replacementLayout("changed_sets_checkpoint_parsed", "checkpoint_parsed")
  }

  test("mapped_name_commits") {
    replacementLayout("mapped_name_commits", "commits", Some("name"))
  }

  test("mapped_name_checkpoint_json") {
    replacementLayout("mapped_name_checkpoint_json", "checkpoint_json", Some("name"))
  }

  test("mapped_name_checkpoint_parsed") {
    replacementLayout("mapped_name_checkpoint_parsed", "checkpoint_parsed", Some("name"))
  }

  test("mapped_id_commits") {
    replacementLayout("mapped_id_commits", "commits", Some("id"))
  }

  test("mapped_id_checkpoint_json") {
    replacementLayout("mapped_id_checkpoint_json", "checkpoint_json", Some("id"))
  }

  test("mapped_id_checkpoint_parsed") {
    replacementLayout("mapped_id_checkpoint_parsed", "checkpoint_parsed", Some("id"))
  }

}
