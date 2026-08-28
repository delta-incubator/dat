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

import io.delta.workload.WorkloadTestSuite

/**
 * Interval type coverage.
 */
class IntervalTypesSuite extends WorkloadTestSuite("interval_types") {

  test("001_interval_ym_basic") {
    sql("CREATE TABLE tbl (id INT, period INTERVAL YEAR TO MONTH) USING delta")
    sql("INSERT INTO tbl VALUES (1, INTERVAL '1-6' YEAR TO MONTH)")
    sql("""INSERT INTO tbl VALUES (2, INTERVAL '2-3' YEAR TO MONTH),(3, INTERVAL '0-9' YEAR TO
      MONTH)""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("002_interval_dt_basic") {
    sql("CREATE TABLE tbl (id INT, duration INTERVAL DAY TO SECOND) USING delta")
    sql("INSERT INTO tbl VALUES (1, INTERVAL '1 02:30:00' DAY TO SECOND)")
    sql("""INSERT INTO tbl VALUES (2, INTERVAL '3 06:45:30' DAY TO SECOND),(3, INTERVAL '0 00:15:00'
      DAY TO SECOND)""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("003_interval_partitioned") {
    sql("""CREATE TABLE tbl (id INT, period INTERVAL YEAR TO MONTH) USING delta
      PARTITIONED BY (period)""")
    sql("INSERT INTO tbl VALUES (1, INTERVAL '1-0' YEAR TO MONTH)")
    sql("INSERT INTO tbl VALUES (2, INTERVAL '2-0' YEAR TO MONTH)")
    sql("INSERT INTO tbl VALUES (3, INTERVAL '1-0' YEAR TO MONTH)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("004_interval_negative") {
    sql("CREATE TABLE tbl (id INT, period INTERVAL YEAR TO MONTH) USING delta")
    sql("INSERT INTO tbl VALUES (1, INTERVAL '-1-6' YEAR TO MONTH)")
    sql("INSERT INTO tbl VALUES (2, INTERVAL '-0-3' YEAR TO MONTH)")
    sql("INSERT INTO tbl VALUES (3, INTERVAL '0-0' YEAR TO MONTH)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("005_interval_mixed") {
    sql("""CREATE TABLE tbl (
      id INT,
      period INTERVAL YEAR TO MONTH,
      duration INTERVAL DAY TO SECOND
    ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1, INTERVAL '1-0' YEAR TO MONTH, INTERVAL '1 00:00:00' DAY TO SECOND),
      (2, INTERVAL '0-6' YEAR TO MONTH, INTERVAL '0 12:30:00' DAY TO SECOND)""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("006_interval_nulls") {
    sql("""CREATE TABLE tbl (
      id INT,
      period INTERVAL YEAR TO MONTH,
      duration INTERVAL DAY TO SECOND
    ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1,
        INTERVAL '1-2' YEAR TO MONTH, INTERVAL '3 04:05:06.000007' DAY TO SECOND),
      (2, NULL, INTERVAL '0 00:00:01.000000' DAY TO SECOND),
      (3, INTERVAL '2-0' YEAR TO MONTH, NULL),
      (4, NULL, NULL)""")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    snapshotSpec(t)
  }

  test("007_interval_mixed_types") {
    sql("""CREATE TABLE tbl (
      id INT,
      label STRING,
      active BOOLEAN,
      amount DECIMAL(10,2),
      period INTERVAL YEAR TO MONTH,
      duration INTERVAL DAY TO SECOND
    ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1, 'alpha', true, 12.34,
        INTERVAL '1-0' YEAR TO MONTH, INTERVAL '0 01:02:03.000004' DAY TO SECOND),
      (2, 'beta', false, -56.78,
        INTERVAL '0-6' YEAR TO MONTH, INTERVAL '2 00:00:00.000000' DAY TO SECOND),
      (3, 'gamma', true, 0.00,
        INTERVAL '3-3' YEAR TO MONTH, INTERVAL '0 00:00:00.000001' DAY TO SECOND)""")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    snapshotSpec(t)
  }

  test("008_interval_nested_struct") {
    sql("""CREATE TABLE tbl (
      id INT,
      info STRUCT<
        label: STRING,
        period: INTERVAL YEAR TO MONTH,
        duration: INTERVAL DAY TO SECOND>
    ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1, named_struct(
        'label', 'both',
        'period', INTERVAL '1-1' YEAR TO MONTH,
        'duration', INTERVAL '0 02:00:00.000000' DAY TO SECOND)),
      (2, named_struct(
        'label', 'null_period',
        'period', CAST(NULL AS INTERVAL YEAR TO MONTH),
        'duration', INTERVAL '1 00:00:00.000000' DAY TO SECOND)),
      (3, named_struct(
        'label', 'null_duration',
        'period', INTERVAL '0-3' YEAR TO MONTH,
        'duration', CAST(NULL AS INTERVAL DAY TO SECOND)))""")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    snapshotSpec(t)
  }

  test("009_interval_array") {
    sql("""CREATE TABLE tbl (
      id INT,
      periods ARRAY<INTERVAL YEAR TO MONTH>,
      durations ARRAY<INTERVAL DAY TO SECOND>
    ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1,
        array(INTERVAL '1-0' YEAR TO MONTH, INTERVAL '0-6' YEAR TO MONTH),
        array(INTERVAL '0 01:00:00.000000' DAY TO SECOND,
          INTERVAL '1 00:00:00.000000' DAY TO SECOND)),
      (2,
        array(INTERVAL '2-3' YEAR TO MONTH, CAST(NULL AS INTERVAL YEAR TO MONTH)),
        array(CAST(NULL AS INTERVAL DAY TO SECOND),
          INTERVAL '0 00:00:01.000000' DAY TO SECOND))""")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    snapshotSpec(t)
  }

  test("010_interval_map_key") {
    sql("""CREATE TABLE tbl (
      id INT,
      ym_labels MAP<INTERVAL YEAR TO MONTH, STRING>,
      dt_labels MAP<INTERVAL DAY TO SECOND, STRING>
    ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1,
        map(INTERVAL '1-0' YEAR TO MONTH, 'one_year',
          INTERVAL '0-6' YEAR TO MONTH, 'six_months'),
        map(INTERVAL '0 01:00:00.000000' DAY TO SECOND, 'one_hour',
          INTERVAL '1 00:00:00.000000' DAY TO SECOND, 'one_day')),
      (2,
        map(INTERVAL '2-3' YEAR TO MONTH, 'two_years_three_months'),
        map(INTERVAL '0 00:00:01.000000' DAY TO SECOND, 'one_second'))""")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    snapshotSpec(t)
  }

  test("011_interval_map_value") {
    sql("""CREATE TABLE tbl (
      id INT,
      ym_values MAP<STRING, INTERVAL YEAR TO MONTH>,
      dt_values MAP<STRING, INTERVAL DAY TO SECOND>
    ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1,
        map('primary', INTERVAL '1-0' YEAR TO MONTH,
          'secondary', INTERVAL '0-6' YEAR TO MONTH),
        map('primary', INTERVAL '0 01:00:00.000000' DAY TO SECOND,
          'secondary', INTERVAL '1 00:00:00.000000' DAY TO SECOND)),
      (2,
        map('primary', INTERVAL '2-3' YEAR TO MONTH,
          'missing', CAST(NULL AS INTERVAL YEAR TO MONTH)),
        map('primary', CAST(NULL AS INTERVAL DAY TO SECOND),
          'secondary', INTERVAL '0 00:00:01.000000' DAY TO SECOND))""")
    val t = registerTable("tbl")
    readSpec(t, name = "read_all")
    snapshotSpec(t)
  }

  test("012_boundary_values") {
    sql("""CREATE TABLE tbl (
      id INT,
      period INTERVAL YEAR TO MONTH,
      duration INTERVAL DAY TO SECOND
    ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1, INTERVAL '178956970-7' YEAR TO MONTH, INTERVAL '106751991 04:00:54.775807' DAY TO SECOND),
      (2, INTERVAL '-178956970-8' YEAR TO MONTH, INTERVAL '-106751991 04:00:54.775808' DAY TO
        SECOND)""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("013_sub_second") {
    sql("CREATE TABLE tbl (id INT, duration INTERVAL DAY TO SECOND) USING delta")
    sql("""INSERT INTO tbl VALUES
      (1, INTERVAL '0 00:00:00.001' DAY TO SECOND),
      (2, INTERVAL '0 00:00:00.999999' DAY TO SECOND),
      (3, INTERVAL '0 00:00:01.5' DAY TO SECOND)""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

}
