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

import org.apache.spark.sql.types.{DataType, StructType}

import io.delta.workload.{WorkloadContext, WorkloadTestSuite}
import io.delta.workload.engine.RowComparison
import io.delta.workload.model._

/**
 * Portable row semantics for read predicates, without a pruning assertion.
 * Capture and replay use UTC. Signed-zero requires Parquet filter pushdown disabled; three
 * narrowing CAST cases require ANSI disabled. Use `startswith`; `starts_with` is unsupported.
 */
class ReadinessPredicatesSuite extends WorkloadTestSuite("readiness_predicates") {

  for (partitioned <- Seq(false, true)) {
    val layout = if (partitioned) "partitioned" else "unpartitioned"
    test(s"nondeterministic_values_$layout") {
      val partition = if (partitioned) "PARTITIONED BY (p)" else ""
      sql(s"CREATE TABLE tbl (id INT, value INT, p INT) USING delta $partition")
      sql("INSERT INTO tbl VALUES (1, 10, 1), (2, 20, 1), (2, 20, 1)")
      sql("INSERT INTO tbl VALUES (3, 20, 2), (4, 30, 2), (5, 40, 3)")
      sql("INSERT INTO tbl VALUES (6, NULL, 1), (7, 50, NULL), (8, NULL, NULL)")
      val t = registerTable("tbl")
      val table = WorkloadContext.current.getTableSpec(t)

      // Literal bags are independent of the filtered Delta reads, including duplicate/NULL rows.
      val all = Seq(
        "(1, 10, 1)",
        "(2, 20, 1)",
        "(2, 20, 1)",
        "(3, 20, 2)",
        "(4, 30, 2)",
        "(5, 40, 3)",
        "(6, NULL, 1)",
        "(7, 50, NULL)",
        "(8, NULL, NULL)"
      )
      val equality = Seq("(2, 20, 1)", "(2, 20, 1)", "(3, 20, 2)")
      val range = equality :+ "(4, 30, 2)"
      val partitionBag = Seq("(1, 10, 1)", "(2, 20, 1)", "(2, 20, 1)", "(6, NULL, 1)")
      val cases = Seq(
        ("read_all", null, all),
        ("random_all", "rand() >= 0", all),
        ("random_none", "rand() < 0", Seq.empty[String]),
        ("equality_control", "value = 20", equality),
        ("range_control", "value >= 20 AND value < 40", range),
        ("and_equality", "rand() >= 0 AND value = 20", equality),
        ("and_range", "rand() >= 0 AND (value >= 20 AND value < 40)", range),
        ("and_none", "rand() < 0 AND value = 20", Seq.empty[String]),
        ("or_equality", "rand() < 0 OR value = 20", equality),
        ("or_range", "rand() < 0 OR (value >= 20 AND value < 40)", range),
        ("or_all", "rand() >= 0 OR value = 20", all),
        ("partition_control", "p = 1", partitionBag),
        ("and_partition", "p = 1 AND rand() >= 0", partitionBag),
        ("or_partition", "p = 1 OR rand() < 0", partitionBag),
        // Spark can simplify the domain-only controls. Referencing the row's small exact INT id
        // keeps a nondeterministic expression at read time: adding [0, 1) cannot decrease id.
        ("row_random_all", "rand() + id >= id", all),
        ("row_random_none", "rand() + id < id", Seq.empty[String]),
        ("row_and_equality", "rand() + id >= id AND value = 20", equality),
        ("row_and_range", "rand() + id >= id AND (value >= 20 AND value < 40)", range),
        ("row_or_equality", "rand() + id < id OR value = 20", equality),
        ("row_or_range", "rand() + id < id OR (value >= 20 AND value < 40)", range),
        ("row_and_partition", "p = 1 AND rand() + id >= id", partitionBag),
        ("row_or_partition", "p = 1 OR rand() + id < id", partitionBag)
      )

      // rand() stays in the serialized read predicate. Its [0, 1) domain gives a stable bag
      // across RNG seeds, partition counts and replays; random values are never stored on INSERT.
      for ((label, predicate, bag) <- cases) {
        readSpec(t, predicate = predicate, name = Some(label)).assert { spec =>
          spec.expectation match {
            case Succeeded(_) =>
            case other => fail(s"${table.outputName}/$label must succeed: $other")
          }
          val oracle = if (bag.nonEmpty) {
            s"SELECT CAST(id AS INT) AS id, CAST(value AS INT) AS value, " +
            s"CAST(p AS INT) AS p FROM VALUES ${bag.mkString(", ")} AS expected(id, value, p)"
          } else {
            "SELECT CAST(NULL AS INT) AS id, CAST(NULL AS INT) AS value, " +
            "CAST(NULL AS INT) AS p WHERE false"
          }
          val expectedPath = outputDir
            .resolve(table.outputName)
            .resolve("expected")
            .resolve(s"${table.outputName}_$label")
            .resolve("expected_data")
          RowComparison.assertRowsEqual(
            spark.sql(oracle),
            spark.read.parquet(expectedPath.toString),
            s"${table.outputName}/$label literal oracle"
          )
        }
      }

      snapshotSpec(t).assert { spec =>
        spec.expectation match {
          case Succeeded(result) =>
            val schema = DataType.fromJson(result.metadata.schemaString).asInstanceOf[StructType]
            require(schema == StructType.fromDDL("id INT, value INT, p INT"))
            require(
              result.metadata.partitionColumns ==
              (if (partitioned) Seq("p") else Seq.empty)
            )
          case other => fail(s"${table.outputName} snapshot must succeed: $other")
        }
      }
    }
  }

  test("signed_zero") {
    require(
      spark.conf.get("spark.sql.parquet.filterPushdown") == "false",
      "The signed-zero bag is validated with Parquet filter pushdown disabled"
    )
    sql("CREATE TABLE tbl (id INT, f FLOAT, d DOUBLE) USING delta")
    // Separate writes keep the two zero signs and nonzero ranges in distinct files.
    sql("""INSERT INTO tbl VALUES
      |(1, CAST('0.0' AS FLOAT), CAST('0.0' AS DOUBLE)),
      |(2, CAST('0.0' AS FLOAT), CAST('0.0' AS DOUBLE)),
      |(2, CAST('0.0' AS FLOAT), CAST('0.0' AS DOUBLE))""".stripMargin)
    sql("""INSERT INTO tbl VALUES
      |(3, CAST('-0.0' AS FLOAT), CAST('-0.0' AS DOUBLE)),
      |(4, CAST('-0.0' AS FLOAT), CAST('-0.0' AS DOUBLE)),
      |(4, CAST('-0.0' AS FLOAT), CAST('-0.0' AS DOUBLE))""".stripMargin)
    sql("INSERT INTO tbl VALUES (5, -1, -1), (6, -2, -2)")
    sql("INSERT INTO tbl VALUES (7, 1, 1), (8, 2, 2)")
    sql("INSERT INTO tbl VALUES (9, NULL, NULL)")
    val t = registerTable("tbl")
    val table = WorkloadContext.current.getTableSpec(t)

    // Literal bags do not filter the source table. IDs distinguish the original zero signs;
    // exact duplicates check multiplicity. Row equality need not retain a zero's sign bit.
    val zeros = Seq("(1, 0, 0)", "(2, 0, 0)", "(2, 0, 0)", "(3, 0, 0)", "(4, 0, 0)", "(4, 0, 0)")
    val negative = Seq("(5, -1, -1)", "(6, -2, -2)")
    val positive = Seq("(7, 1, 1)", "(8, 2, 2)")
    val nulls = Seq("(9, NULL, NULL)")
    val nonzero = negative ++ positive
    val all = zeros ++ nonzero ++ nulls
    val cases = Seq(("read_all", null, all)) ++
      Seq(("f", "FLOAT"), ("d", "DOUBLE")).flatMap {
        case (column, dataType) =>
          val plus = s"CAST('0.0' AS $dataType)"
          val minus = s"CAST('-0.0' AS $dataType)"
          val one = s"CAST(1 AS $dataType)"
          val negativeOne = s"CAST(-1 AS $dataType)"
          val typedNull = s"CAST(NULL AS $dataType)"
          val comparisons = for {
            (sign, zero) <- Seq(("positive", plus), ("negative", minus))
            (label, operator, bag) <- Seq(
              ("eq", "=", zeros),
              ("null_safe_eq", "<=>", zeros),
              ("ne", "!=", nonzero),
              ("lt", "<", negative),
              ("le", "<=", negative ++ zeros),
              ("gt", ">", positive),
              ("ge", ">=", zeros ++ positive)
            )
          } yield (s"${column}_${label}_$sign", s"$column $operator $zero", bag)
          val inLists = Seq(
            ("in_positive", plus, zeros),
            ("in_negative", minus, zeros),
            ("in_both", s"$plus, $minus", zeros),
            ("in_positive_null", s"$plus, $typedNull", zeros),
            ("in_negative_null", s"$minus, $typedNull", zeros),
            ("in_both_null", s"$plus, $minus, $typedNull", zeros),
            ("in_positive_nonzero", s"$plus, $one", zeros :+ "(7, 1, 1)"),
            ("in_negative_nonzero", s"$minus, $negativeOne", zeros :+ "(5, -1, -1)"),
            (
              "in_both_null_nonzero",
              s"$plus, $minus, $typedNull, $negativeOne, $one",
              zeros ++ Seq("(5, -1, -1)", "(7, 1, 1)")
            ),
            ("in_nonzero_null", s"$negativeOne, $one, $typedNull", Seq("(5, -1, -1)", "(7, 1, 1)")),
            ("in_null", typedNull, Seq.empty[String]),
            ("in_miss", s"CAST(9 AS $dataType), $typedNull", Seq.empty[String])
          )
          comparisons ++ Seq((s"${column}_null_safe_null", s"$column <=> $typedNull", nulls)) ++
          inLists.map {
            case (label, literals, bag) =>
              (s"${column}_$label", s"$column IN ($literals)", bag)
          }
      }

    for ((label, predicate, bag) <- cases) {
      readSpec(t, predicate = predicate, name = Some(label)).assert { spec =>
        spec.expectation match {
          case Succeeded(result) => require(result.rowCount == bag.size)
          case other => fail(s"${table.outputName}/$label must succeed: $other")
        }
        val oracle = if (bag.nonEmpty) {
          "SELECT CAST(id AS INT) AS id, CAST(f AS FLOAT) AS f, CAST(d AS DOUBLE) AS d " +
          s"FROM VALUES ${bag.mkString(", ")} AS expected(id, f, d)"
        } else {
          "SELECT CAST(NULL AS INT) AS id, CAST(NULL AS FLOAT) AS f, " +
          "CAST(NULL AS DOUBLE) AS d WHERE false"
        }
        val expectedPath = outputDir
          .resolve(table.outputName)
          .resolve("expected")
          .resolve(s"${table.outputName}_$label")
          .resolve("expected_data")
        RowComparison.assertRowsEqual(
          spark.sql(oracle),
          spark.read.parquet(expectedPath.toString),
          s"${table.outputName}/$label literal oracle"
        )
      }
    }

    snapshotSpec(t).assert { spec =>
      spec.expectation match {
        case Succeeded(result) =>
          val schema = DataType.fromJson(result.metadata.schemaString).asInstanceOf[StructType]
          require(schema == StructType.fromDDL("id INT, f FLOAT, d DOUBLE"))
          require(result.metadata.partitionColumns.isEmpty)
        case other => fail(s"${table.outputName} snapshot must succeed: $other")
      }
    }
  }

  // Explicit predicate casts, precision boundaries and legacy narrowing.
  // Expected IDs are derived independently of the CAST predicates. Source duplicates stay intact.
  for ((subcase, sourceType, sourceRows, checks, legacy) <- Seq(
      (
        "float_double",
        "FLOAT",
        Seq(
          (1, "-0.1"),
          (2, "0.1"),
          (2, "0.1"),
          (3, "0.10000001"),
          (4, "0.2"),
          (5, "1.0"),
          (6, "NULL")
        ),
        Seq(
          ("read_all", null, Seq(1, 2, 3, 4, 5, 6)),
          ("read_hit", "CAST(value AS DOUBLE) = CAST('0.10000000149011612' AS DOUBLE)", Seq(2)),
          (
            "decimal_spelling_miss",
            "CAST(value AS DOUBLE) = CAST('0.1' AS DOUBLE)",
            Seq.empty[Int]
          ),
          ("above_decimal", "CAST(value AS DOUBLE) > CAST('0.1' AS DOUBLE)", Seq(2, 3, 4, 5)),
          (
            "adjacent_range",
            "CAST(value AS DOUBLE) >= CAST('0.10000000149011612' AS DOUBLE) AND " +
            "CAST(value AS DOUBLE) < CAST('0.10000000894069672' AS DOUBLE)",
            Seq(2)
          ),
          (
            "negative_hit",
            "CAST(value AS DOUBLE) = CAST('-0.10000000149011612' AS DOUBLE)",
            Seq(1)
          ),
          ("read_miss", "CAST(value AS DOUBLE) = CAST('2' AS DOUBLE)", Seq.empty[Int])
        ),
        false
      ),
      (
        "double_2p53",
        "BIGINT",
        Seq(
          (1, "9007199254740991"),
          (2, "9007199254740992"),
          (3, "9007199254740993"),
          (3, "9007199254740993"),
          (4, "9007199254740994"),
          (5, "9007199254740995"),
          (6, "-9007199254740993"),
          (7, "NULL")
        ),
        Seq(
          ("read_all", null, Seq(1, 2, 3, 4, 5, 6, 7)),
          ("read_hit", "CAST(value AS DOUBLE) = CAST('9007199254740992' AS DOUBLE)", Seq(2, 3)),
          ("adjacent_hit", "CAST(value AS DOUBLE) = CAST('9007199254740994' AS DOUBLE)", Seq(4)),
          (
            "adjacent_range",
            "CAST(value AS DOUBLE) > CAST('9007199254740992' AS DOUBLE) AND " +
            "CAST(value AS DOUBLE) < CAST('9007199254740996' AS DOUBLE)",
            Seq(4)
          ),
          (
            "below_boundary",
            "CAST(value AS DOUBLE) < CAST('9007199254740992' AS DOUBLE)",
            Seq(1, 6)
          ),
          (
            "read_miss",
            "CAST(value AS DOUBLE) = CAST('9007199254740998' AS DOUBLE)",
            Seq.empty[Int]
          )
        ),
        false
      ),
      (
        "float_2p24",
        "INT",
        Seq(
          (1, "16777215"),
          (2, "16777216"),
          (3, "16777217"),
          (3, "16777217"),
          (4, "16777218"),
          (5, "16777219"),
          (6, "-16777217"),
          (7, "NULL")
        ),
        Seq(
          ("read_all", null, Seq(1, 2, 3, 4, 5, 6, 7)),
          ("read_hit", "CAST(value AS FLOAT) = CAST('16777216' AS FLOAT)", Seq(2, 3)),
          ("adjacent_hit", "CAST(value AS FLOAT) = CAST('16777218' AS FLOAT)", Seq(4)),
          (
            "adjacent_range",
            "CAST(value AS FLOAT) > CAST('16777216' AS FLOAT) AND " +
            "CAST(value AS FLOAT) < CAST('16777220' AS FLOAT)",
            Seq(4)
          ),
          ("below_boundary", "CAST(value AS FLOAT) < CAST('16777216' AS FLOAT)", Seq(1, 6)),
          ("read_miss", "CAST(value AS FLOAT) = CAST('16777222' AS FLOAT)", Seq.empty[Int])
        ),
        false
      ),
      (
        "long_int_wrap",
        "BIGINT",
        Seq(
          (1, "4"),
          (2, "5"),
          (2, "5"),
          (3, "4294967301"),
          (3, "4294967301"),
          (4, "4294967300"),
          (5, "4294967302"),
          (6, "-4294967291"),
          (7, "NULL"),
          (8, "-1"),
          (9, "6")
        ),
        Seq(
          ("read_all", null, Seq(1, 2, 3, 4, 5, 6, 7, 8, 9)),
          ("read_hit", "CAST(value AS INT) = CAST('5' AS INT)", Seq(2, 3, 6)),
          ("lower_neighbor", "CAST(value AS INT) = CAST('4' AS INT)", Seq(1, 4)),
          ("upper_neighbor", "CAST(value AS INT) = CAST('6' AS INT)", Seq(5, 9)),
          ("below_five", "CAST(value AS INT) < CAST('5' AS INT)", Seq(1, 4, 8)),
          ("above_five", "CAST(value AS INT) > CAST('5' AS INT)", Seq(5, 9)),
          ("read_miss", "CAST(value AS INT) = CAST('7' AS INT)", Seq.empty[Int])
        ),
        true
      ),
      (
        "to_short_wrap",
        "INT",
        Seq(
          (1, "4"),
          (2, "5"),
          (2, "5"),
          (3, "65541"),
          (3, "65541"),
          (4, "65540"),
          (5, "65542"),
          (6, "-65531"),
          (7, "NULL"),
          (8, "-1"),
          (9, "6")
        ),
        Seq(
          ("read_all", null, Seq(1, 2, 3, 4, 5, 6, 7, 8, 9)),
          ("read_hit", "CAST(value AS SHORT) = CAST('5' AS SHORT)", Seq(2, 3, 6)),
          ("lower_neighbor", "CAST(value AS SHORT) = CAST('4' AS SHORT)", Seq(1, 4)),
          ("upper_neighbor", "CAST(value AS SHORT) = CAST('6' AS SHORT)", Seq(5, 9)),
          ("below_five", "CAST(value AS SHORT) < CAST('5' AS SHORT)", Seq(1, 4, 8)),
          ("above_five", "CAST(value AS SHORT) > CAST('5' AS SHORT)", Seq(5, 9)),
          ("read_miss", "CAST(value AS SHORT) = CAST('7' AS SHORT)", Seq.empty[Int])
        ),
        true
      ),
      (
        "int_byte_wrap",
        "INT",
        Seq(
          (1, "4"),
          (2, "5"),
          (2, "5"),
          (3, "261"),
          (3, "261"),
          (4, "260"),
          (5, "262"),
          (6, "-251"),
          (7, "NULL"),
          (8, "-1"),
          (9, "6")
        ),
        Seq(
          ("read_all", null, Seq(1, 2, 3, 4, 5, 6, 7, 8, 9)),
          ("read_hit", "CAST(value AS BYTE) = CAST('5' AS BYTE)", Seq(2, 3, 6)),
          ("lower_neighbor", "CAST(value AS BYTE) = CAST('4' AS BYTE)", Seq(1, 4)),
          ("upper_neighbor", "CAST(value AS BYTE) = CAST('6' AS BYTE)", Seq(5, 9)),
          ("below_five", "CAST(value AS BYTE) < CAST('5' AS BYTE)", Seq(1, 4, 8)),
          ("above_five", "CAST(value AS BYTE) > CAST('5' AS BYTE)", Seq(5, 9)),
          ("read_miss", "CAST(value AS BYTE) = CAST('7' AS BYTE)", Seq.empty[Int])
        ),
        true
      )
    )) {
    test(s"cast_filters_$subcase") {
      // ReadQuery has no session config field. Supply the same legacy session on every replay.
      if (legacy) {
        require(
          spark.conf.get("spark.sql.ansi.enabled") == "false",
          "Narrowing CAST fixtures require an existing non-ANSI session for capture and replay"
        )
      }
      sql(s"CREATE TABLE tbl (id INT, value $sourceType) USING delta")
      // Separate writes leave ordinary and boundary values in multiple stats-bearing files.
      for (batch <- sourceRows.grouped(2)) {
        val values = batch.map { case (id, value) => s"($id, $value)" }.mkString(", ")
        sql(s"INSERT INTO tbl VALUES $values")
      }
      val t = registerTable("tbl")
      val table = WorkloadContext.current.getTableSpec(t)
      for ((label, predicate, expectedIds) <- checks) {
        val bag = sourceRows.filter { case (id, _) => expectedIds.contains(id) }
        readSpec(t, predicate = predicate, name = Some(label)).assert { spec =>
          spec.expectation match {
            case Succeeded(result) => require(result.rowCount == bag.size)
            case other => fail(s"${table.outputName}/$label must succeed: $other")
          }
          // Only source-type casts construct these literal bags; no predicate CAST is reused.
          val oracle = if (bag.nonEmpty) {
            val values = bag.map { case (id, value) => s"($id, $value)" }.mkString(", ")
            s"SELECT CAST(id AS INT) AS id, CAST(value AS $sourceType) AS value " +
            s"FROM VALUES $values AS expected(id, value)"
          } else {
            s"SELECT CAST(NULL AS INT) AS id, CAST(NULL AS $sourceType) AS value WHERE false"
          }
          val expectedPath = outputDir
            .resolve(table.outputName)
            .resolve("expected")
            .resolve(s"${table.outputName}_$label")
            .resolve("expected_data")
          RowComparison.assertRowsEqual(
            spark.sql(oracle),
            spark.read.parquet(expectedPath.toString),
            s"${table.outputName}/$label literal oracle"
          )
        }
      }
      snapshotSpec(t).assert { spec =>
        spec.expectation match {
          case Succeeded(result) =>
            val schema = DataType.fromJson(result.metadata.schemaString).asInstanceOf[StructType]
            require(schema == StructType.fromDDL(s"id INT, value $sourceType"))
            require(result.metadata.partitionColumns.isEmpty)
            require(result.metadata.configuration.isEmpty)
            require(result.protocol.minReaderVersion == 1)
            require(result.protocol.minWriterVersion == 2)
          case other => fail(s"${table.outputName} snapshot must succeed: $other")
        }
      }
      if (subcase == "float_double") {
        val compositionCases = Seq(
          ("direct_nonnegative", "CAST(value AS DOUBLE) >= 0", Seq(2, 3, 4, 5)),
          (
            "compound_hit",
            "(CAST(value AS DOUBLE) > 0.15 AND id < 5) OR " +
              "(CAST(value AS DOUBLE) < 0 AND id = 1)",
            Seq(1, 4)
          ),
          (
            "compound_miss",
            "(CAST(value AS DOUBLE) > 2 AND id < 5) OR " +
              "(CAST(value AS DOUBLE) < -2 AND id = 1)",
            Seq.empty[Int]
          )
        )
        for ((label, predicate, ids) <- compositionCases) {
          val bag = sourceRows.filter { case (id, _) => ids.contains(id) }
          readSpec(t, predicate = predicate, name = Some(label)).assert { spec =>
            spec.expectation match {
              case Succeeded(result) => require(result.rowCount == bag.size)
              case other => fail(s"${table.outputName}/$label must succeed: $other")
            }
            val oracle = if (bag.nonEmpty) {
              val values = bag.map { case (id, value) => s"($id, $value)" }.mkString(", ")
              s"SELECT CAST(id AS INT) AS id, CAST(value AS FLOAT) AS value " +
                s"FROM VALUES $values AS expected(id, value)"
            } else {
              "SELECT CAST(NULL AS INT) AS id, CAST(NULL AS FLOAT) AS value WHERE false"
            }
            val expectedPath = outputDir
              .resolve(table.outputName)
              .resolve("expected")
              .resolve(s"${table.outputName}_$label")
              .resolve("expected_data")
            RowComparison.assertRowsEqual(
              spark.sql(oracle),
              spark.read.parquet(expectedPath.toString),
              s"${table.outputName}/$label literal oracle"
            )
          }
        }
      }

    }
  }

  // Complex filter row semantics. File counts do not prove filter pushdown.
  test("complex_filters_month_timestamp") {
    // Read specs have no timezone field; use UTC through queued capture and every replay.
    require(spark.conf.get("spark.sql.session.timeZone") == "UTC")
    sql("CREATE TABLE tbl (id INT, ts TIMESTAMP) USING delta")
    val rows = Seq(
      "(1, TIMESTAMP'2023-12-31 23:59:59.999999')",
      "(2, TIMESTAMP'2024-01-01 00:00:00')",
      "(3, TIMESTAMP'2024-01-31 23:59:59.999999')",
      "(4, TIMESTAMP'2024-02-01 00:00:00')",
      "(5, TIMESTAMP'2024-02-29 23:59:59.999999')",
      "(6, TIMESTAMP'2024-03-01 00:00:00')",
      "(7, TIMESTAMP'2024-12-31 23:59:59.999999')",
      "(8, TIMESTAMP'2025-01-01 00:00:00')",
      "(8, TIMESTAMP'2025-01-01 00:00:00')",
      "(9, NULL)"
    )
    for (batch <- rows.grouped(2)) {
      sql(s"INSERT INTO tbl VALUES ${batch.mkString(", ")}")
    }
    val t = registerTable("tbl")
    val table = WorkloadContext.current.getTableSpec(t)
    // Enumerated source IDs preserve duplicates without evaluating the predicate in the oracle.
    val cases = Seq(
      ("read_all", null, Seq(1, 2, 3, 4, 5, 6, 7, 8, 9)),
      ("january", "month(ts) = 1", Seq(2, 3, 8)),
      ("february", "month(ts) = 2", Seq(4, 5)),
      ("december", "month(ts) = 12", Seq(1, 7)),
      ("miss_june", "month(ts) = 6", Seq.empty[Int]),
      ("null_month", "month(ts) IS NULL", Seq(9))
    )
    for ((label, predicate, ids) <- cases) {
      val bag = rows.filter(row => ids.contains(row.drop(1).takeWhile(_ != ',').toInt))
      readSpec(t, predicate = predicate, name = Some(label)).assert { spec =>
        spec.expectation match {
          case Succeeded(result) => require(result.rowCount == bag.size)
          case other => fail(s"${table.outputName}/$label must succeed: $other")
        }
        val values = if (bag.nonEmpty) bag.mkString(", ") else "(NULL, NULL)"
        val projection = "CAST(id AS INT) AS id, CAST(ts AS TIMESTAMP) AS ts"
        val oracle = s"SELECT $projection FROM VALUES $values AS expected(id, ts)" +
          (if (bag.nonEmpty) "" else " WHERE false")
        val expectedPath = outputDir
          .resolve(table.outputName)
          .resolve("expected")
          .resolve(s"${table.outputName}_$label")
          .resolve("expected_data")
        RowComparison.assertRowsEqual(
          spark.sql(oracle),
          spark.read.parquet(expectedPath.toString),
          s"${table.outputName}/$label literal oracle"
        )
      }
    }
    snapshotSpec(t).assert { spec =>
      spec.expectation match {
        case Succeeded(result) =>
          val schema = DataType.fromJson(result.metadata.schemaString).asInstanceOf[StructType]
          require(schema == StructType.fromDDL("id INT, ts TIMESTAMP"))
          require(result.metadata.partitionColumns.isEmpty)
          require(result.metadata.configuration.isEmpty)
          require(result.protocol.minReaderVersion == 1)
          require(result.protocol.minWriterVersion == 2)
        case other => fail(s"${table.outputName} snapshot must succeed: $other")
      }
    }
  }

  test("complex_filters_strings") {
    sql("CREATE TABLE tbl (id INT, name STRING, s STRUCT<name: STRING>) USING delta")
    val rows = Seq(
      "(1, 'apple', named_struct('name', 'apple'))",
      "(2, 'banana', named_struct('name', 'banana'))",
      "(3, 'alice', named_struct('name', 'alice'))",
      "(4, 'bob', named_struct('name', 'bob'))",
      "(5, 'charlie', named_struct('name', 'charlie'))",
      "(6, 'diana', named_struct('name', 'diana'))",
      "(7, 'Apple', named_struct('name', 'APPLE'))",
      "(8, 'ALICE', named_struct('name', 'Alice'))",
      "(8, 'ALICE', named_struct('name', 'Alice'))",
      "(9, '', named_struct('name', ''))",
      "(10, NULL, named_struct('name', NULL))",
      "(11, 'apPle', named_struct('name', NULL))"
    )
    for (batch <- rows.grouped(2)) {
      sql(s"INSERT INTO tbl VALUES ${batch.mkString(", ")}")
    }
    val t = registerTable("tbl")
    val table = WorkloadContext.current.getTableSpec(t)
    // Enumerated source IDs preserve duplicates without evaluating the predicate in the oracle.
    val cases = Seq(
      ("read_all", null, Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)),
      ("prefix_hit", "startswith(name, 'app')", Seq(1)),
      ("prefix_a", "startswith(name, 'a')", Seq(1, 3, 11)),
      ("prefix_nested", "startswith(s.name, 'a')", Seq(1, 3)),
      ("prefix_miss", "startswith(name, 'z')", Seq.empty[Int]),
      ("prefix_empty", "startswith(name, '')", Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 11)),
      ("prefix_null", "startswith(name, CAST(NULL AS STRING))", Seq.empty[Int]),
      ("prefix_null_result", "startswith(name, 'a') IS NULL", Seq(10)),
      ("upper_alice", "upper(name) = 'ALICE'", Seq(3, 8)),
      ("upper_apple", "upper(name) = 'APPLE'", Seq(1, 7, 11)),
      ("upper_miss", "upper(name) = 'ZEBRA'", Seq.empty[Int]),
      ("upper_null", "upper(name) IS NULL", Seq(10)),
      ("upper_selective", "upper(name) = 'APPLE' AND id > 1", Seq(7, 11))
    )
    for ((label, predicate, ids) <- cases) {
      val bag = rows.filter(row => ids.contains(row.drop(1).takeWhile(_ != ',').toInt))
      readSpec(t, predicate = predicate, name = Some(label)).assert { spec =>
        spec.expectation match {
          case Succeeded(result) => require(result.rowCount == bag.size)
          case other => fail(s"${table.outputName}/$label must succeed: $other")
        }
        val values = if (bag.nonEmpty) bag.mkString(", ") else "(NULL, NULL, NULL)"
        val projection = "CAST(id AS INT) AS id, CAST(name AS STRING) AS name, " +
          "CAST(s AS STRUCT<name: STRING>) AS s"
        val oracle = s"SELECT $projection FROM VALUES $values AS expected(id, name, s)" +
          (if (bag.nonEmpty) "" else " WHERE false")
        val expectedPath = outputDir
          .resolve(table.outputName)
          .resolve("expected")
          .resolve(s"${table.outputName}_$label")
          .resolve("expected_data")
        RowComparison.assertRowsEqual(
          spark.sql(oracle),
          spark.read.parquet(expectedPath.toString),
          s"${table.outputName}/$label literal oracle"
        )
      }
    }
    snapshotSpec(t).assert { spec =>
      spec.expectation match {
        case Succeeded(result) =>
          val schema = DataType.fromJson(result.metadata.schemaString).asInstanceOf[StructType]
          require(schema == StructType.fromDDL("id INT, name STRING, s STRUCT<name: STRING>"))
          require(result.metadata.partitionColumns.isEmpty)
          require(result.metadata.configuration.isEmpty)
          require(result.protocol.minReaderVersion == 1)
          require(result.protocol.minWriterVersion == 2)
        case other => fail(s"${table.outputName} snapshot must succeed: $other")
      }
    }
  }

  test("complex_filters_deeply_nested") {
    sql(
      "CREATE TABLE tbl (id INT, s STRUCT<a: INT, b: INT, c: INT, d: INT, e: INT, " +
      "f: INT, g: INT, h: INT, i: INT, j: INT>) USING delta"
    )
    val rows = Seq(
      "(1, named_struct('a', 1, 'b', 1, 'c', NULL, 'd', NULL, 'e', NULL, 'f', NULL, " +
      "'g', NULL, 'h', NULL, 'i', NULL, 'j', NULL))",
      "(2, named_struct('a', 1, 'b', 0, 'c', 0, 'd', 1, 'e', NULL, 'f', NULL, 'g', " +
      "NULL, 'h', NULL, 'i', NULL, 'j', NULL))",
      "(3, named_struct('a', 1, 'b', 0, 'c', 0, 'd', 0, 'e', 1, 'f', 1, 'g', NULL, " +
      "'h', NULL, 'i', NULL, 'j', NULL))",
      "(4, named_struct('a', 1, 'b', 0, 'c', 0, 'd', 0, 'e', 1, 'f', 0, 'g', 0, 'h', " +
      "1, 'i', NULL, 'j', NULL))",
      "(5, named_struct('a', 1, 'b', 0, 'c', 0, 'd', 0, 'e', 1, 'f', 0, 'g', 0, 'h', " +
      "0, 'i', 1, 'j', 0))",
      "(5, named_struct('a', 1, 'b', 0, 'c', 0, 'd', 0, 'e', 1, 'f', 0, 'g', 0, 'h', " +
      "0, 'i', 1, 'j', 0))",
      "(6, named_struct('a', 0, 'b', 1, 'c', 0, 'd', 1, 'e', 1, 'f', 1, 'g', 0, 'h', " +
      "1, 'i', 1, 'j', 0))",
      "(7, named_struct('a', 1, 'b', 0, 'c', 1, 'd', 1, 'e', 1, 'f', 1, 'g', 0, 'h', " +
      "1, 'i', 1, 'j', 0))",
      "(8, named_struct('a', 1, 'b', 0, 'c', 0, 'd', 0, 'e', 1, 'f', 0, 'g', 0, 'h', " +
      "0, 'i', 1, 'j', 1))",
      "(9, named_struct('a', 1, 'b', 0, 'c', 0, 'd', 0, 'e', 1, 'f', 0, 'g', 0, 'h', " +
      "0, 'i', 1, 'j', NULL))",
      "(10, named_struct('a', NULL, 'b', 1, 'c', 0, 'd', 1, 'e', 1, 'f', 1, 'g', 0, " +
      "'h', 1, 'i', 1, 'j', 0))",
      "(11, named_struct('a', NULL, 'b', NULL, 'c', NULL, 'd', NULL, 'e', NULL, 'f', " +
      "NULL, 'g', NULL, 'h', NULL, 'i', NULL, 'j', NULL))",
      "(12, named_struct('a', 1, 'b', 0, 'c', 0, 'd', 0, 'e', 0, 'f', 1, 'g', 0, 'h', " +
      "1, 'i', 1, 'j', 0))"
    )
    for (batch <- rows.grouped(2)) {
      sql(s"INSERT INTO tbl VALUES ${batch.mkString(", ")}")
    }
    val t = registerTable("tbl")
    val table = WorkloadContext.current.getTableSpec(t)
    // Enumerated source IDs preserve duplicates without evaluating the predicate in the oracle.
    val cases = Seq(
      ("read_all", null, Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)),
      (
        "deep_hit",
        "s.a = 1 AND (s.b = 1 OR (NOT (s.c = 1) AND (s.d = 1 OR (s.e = 1 AND (s.f = " +
        "1 OR (NOT (s.g = 1) AND (s.h = 1 OR (s.i = 1 AND NOT (s.j = 1)))))))))",
        Seq(1, 2, 3, 4, 5)
      ),
      (
        "deep_false",
        "NOT (s.a = 1 AND (s.b = 1 OR (NOT (s.c = 1) AND (s.d = 1 OR (s.e = 1 AND " +
        "(s.f = 1 OR (NOT (s.g = 1) AND (s.h = 1 OR (s.i = 1 AND NOT (s.j = 1))))))))))",
        Seq(6, 7, 8, 12)
      ),
      (
        "deep_null",
        "(s.a = 1 AND (s.b = 1 OR (NOT (s.c = 1) AND (s.d = 1 OR (s.e = 1 AND (s.f = " +
        "1 OR (NOT (s.g = 1) AND (s.h = 1 OR (s.i = 1 AND NOT (s.j = 1)))))))))) IS " +
        "NULL",
        Seq(9, 10, 11)
      ),
      (
        "deep_miss",
        "(s.a = 1 AND (s.b = 1 OR (NOT (s.c = 1) AND (s.d = 1 OR (s.e = 1 AND (s.f = " +
        "1 OR (NOT (s.g = 1) AND (s.h = 1 OR (s.i = 1 AND NOT (s.j = 1)))))))))) AND " +
        "id = 99",
        Seq.empty[Int]
      )
    )
    for ((label, predicate, ids) <- cases) {
      val bag = rows.filter(row => ids.contains(row.drop(1).takeWhile(_ != ',').toInt))
      readSpec(t, predicate = predicate, name = Some(label)).assert { spec =>
        spec.expectation match {
          case Succeeded(result) => require(result.rowCount == bag.size)
          case other => fail(s"${table.outputName}/$label must succeed: $other")
        }
        val values = if (bag.nonEmpty) bag.mkString(", ") else "(NULL, NULL)"
        val projection = "CAST(id AS INT) AS id, CAST(s AS STRUCT<a: INT, b: INT, c: INT, " +
          "d: INT, e: INT, f: INT, g: INT, h: INT, i: INT, j: INT>) AS s"
        val oracle = s"SELECT $projection FROM VALUES $values AS expected(id, s)" +
          (if (bag.nonEmpty) "" else " WHERE false")
        val expectedPath = outputDir
          .resolve(table.outputName)
          .resolve("expected")
          .resolve(s"${table.outputName}_$label")
          .resolve("expected_data")
        RowComparison.assertRowsEqual(
          spark.sql(oracle),
          spark.read.parquet(expectedPath.toString),
          s"${table.outputName}/$label literal oracle"
        )
      }
    }
    snapshotSpec(t).assert { spec =>
      spec.expectation match {
        case Succeeded(result) =>
          val schema = DataType.fromJson(result.metadata.schemaString).asInstanceOf[StructType]
          require(
            schema == StructType.fromDDL(
              "id INT, s STRUCT<a: INT, b: INT, c: INT, d: INT, e: INT, f: INT, " +
              "g: INT, h: INT, i: INT, j: INT>"
            )
          )
          require(result.metadata.partitionColumns.isEmpty)
          require(result.metadata.configuration.isEmpty)
          require(result.protocol.minReaderVersion == 1)
          require(result.protocol.minWriterVersion == 2)
        case other => fail(s"${table.outputName} snapshot must succeed: $other")
      }
    }
  }
}
