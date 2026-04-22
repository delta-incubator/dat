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

class CheckConstraintsSuite extends WorkloadTestSuite("check_constraints") {

  test("create_with_constraint") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive_id CHECK (id > 0)")
    sql("INSERT INTO tbl VALUES (1, 'a'),(2, 'b'),(3, 'c')")
    sql("INSERT INTO tbl VALUES (7, 'd'),(8, 'e')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id > 5")
    snapshotSpec(t)
  }

  test("show_tblproperties") {
    sql("""CREATE TABLE tbl (x INT, y INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 10),(2, 20)")
    sql("ALTER TABLE tbl ADD CONSTRAINT myconstraint CHECK (x > 0)")
    sql("INSERT INTO tbl VALUES (5, 50),(6, 60)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "x > 3")
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
    snapshotSpec(t, version = 2)
    snapshotSpec(t, version = 3)
  }

  test("delta_history") {
    sql("""CREATE TABLE tbl (x INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive CHECK (x > 0)")
    sql("INSERT INTO tbl VALUES (4),(5)")
    sql("ALTER TABLE tbl DROP CONSTRAINT positive")
    sql("INSERT INTO tbl VALUES (-1),(0)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 3)
    val N = 5L
    for (v <- 0L to N) snapshotSpec(t, version = v)
  }

  test("case_insensitive_drop") {
    sql("""CREATE TABLE tbl (x INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    sql("ALTER TABLE tbl ADD CONSTRAINT MyConstraint CHECK (x > 0)")
    sql("INSERT INTO tbl VALUES (4),(5)")
    sql("ALTER TABLE tbl DROP CONSTRAINT MYCONSTRAINT")
    // After drop, negative values allowed
    sql("INSERT INTO tbl VALUES (-1),(0)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "x < 0")
    val N = 5L
    for (v <- 0L to N) snapshotSpec(t, version = v)
  }

  test("varchar_constraint") {
    sql("""CREATE TABLE tbl (id INT, s VARCHAR(10)) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'ab'),(2, 'cdef')")
    sql("INSERT INTO tbl VALUES (3, 'ghij')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "length(s) < 4")
    snapshotSpec(t)
  }

  test("basic_constraint") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive_id CHECK (id > 0)")
    sql("INSERT INTO tbl VALUES (1, 'first'),(2, 'second'),(3, 'third')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id = 1")
    snapshotSpec(t)
  }

  test("multiple_constraints") {
    sql("""CREATE TABLE tbl (id INT, amount DECIMAL(10,2), status STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive_id CHECK (id > 0)")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive_amount CHECK (amount >= 0)")
    sql("ALTER TABLE tbl ADD CONSTRAINT valid_status CHECK (status IN ('active', 'pending', 'closed'))")
    sql("INSERT INTO tbl VALUES (1, 10.50, 'active'),(2, 25.00, 'pending'),(3, 0.00, 'closed')")
    sql("INSERT INTO tbl VALUES (4, 100.00, 'active'),(5, 50.75, 'pending')")
    sql("INSERT INTO tbl VALUES (6, 200.00, 'closed')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "amount > 50")
    readSpec(t, predicate = "status = 'active'")
    snapshotSpec(t)
  }

  test("nested_constraint") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT adult CHECK (info.age >= 18)")
    sql("INSERT INTO tbl VALUES (1, named_struct('name', 'Alice', 'age', 25))")
    sql("INSERT INTO tbl VALUES (2, named_struct('name', 'Bob', 'age', 30))")
    sql("INSERT INTO tbl VALUES (3, named_struct('name', 'Charlie', 'age', 18))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "info.age > 27")
    snapshotSpec(t)
  }

  test("array_constraint") {
    sql("""CREATE TABLE tbl (id INT, tags ARRAY<STRING>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT at_least_one_tag CHECK (size(tags) >= 1)")
    sql("INSERT INTO tbl VALUES (1, array('a','b','c'))")
    sql("INSERT INTO tbl VALUES (2, array('x'))")
    sql("INSERT INTO tbl VALUES (3, array('p','q','r','s'))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "size(tags) > 2")
    snapshotSpec(t)
  }

  test("length_constraint") {
    sql("""CREATE TABLE tbl (code STRING, description STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT code_length CHECK (length(code) = 5)")
    sql("INSERT INTO tbl VALUES ('ABCDE', 'first')")
    sql("INSERT INTO tbl VALUES ('FGHIJ', 'second')")
    sql("INSERT INTO tbl VALUES ('KLMNO', 'third')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "code = 'ABCDE'")
    snapshotSpec(t)
  }

  test("compound_constraint") {
    sql("""CREATE TABLE tbl (start_date DATE, end_date DATE, amount DECIMAL(10,2)) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT valid_range CHECK (end_date >= start_date AND amount > 0)")
    sql("INSERT INTO tbl VALUES ('2024-01-01', '2024-01-31', 100.00)")
    sql("INSERT INTO tbl VALUES ('2024-02-01', '2024-02-28', 200.00)")
    sql("INSERT INTO tbl VALUES ('2024-03-01', '2024-03-31', 50.00)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "start_date >= '2024-02-01'")
    snapshotSpec(t)
  }

  test("not_null_constraint") {
    sql("""CREATE TABLE tbl (id INT, required_field STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT required CHECK (required_field IS NOT NULL)")
    sql("INSERT INTO tbl VALUES (1, 'present')")
    sql("INSERT INTO tbl VALUES (2, 'also_present')")
    sql("INSERT INTO tbl VALUES (3, 'here_too')")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("time_travel") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    sql("INSERT INTO tbl VALUES (-1),(0)")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive CHECK (value >= -1)")
    sql("INSERT INTO tbl VALUES (10),(20)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 2)
    val N = 4L
    for (v <- 0L to N) snapshotSpec(t, version = v)
  }

  test("time_type_constraint") {
    sql("""CREATE TABLE tbl (id INT, event_time STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT valid_time CHECK (event_time >= '09:00:00')")
    sql("INSERT INTO tbl VALUES (1, '09:00:00')")
    sql("INSERT INTO tbl VALUES (2, '10:30:00'),(3, '14:00:00')")
    sql("INSERT INTO tbl VALUES (4, '12:00:00'),(5, '17:30:00')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "event_time >= '12:00:00'")
    snapshotSpec(t)
  }

  test("time_multiple_conditions") {
    sql("""CREATE TABLE tbl (id INT, start_time STRING, end_time STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT valid_range CHECK (end_time > start_time)")
    sql("INSERT INTO tbl VALUES (1, '08:00:00', '17:00:00')")
    sql("INSERT INTO tbl VALUES (2, '09:30:00', '18:00:00')")
    sql("INSERT INTO tbl VALUES (3, '06:00:00', '14:00:00')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "start_time < '10:00:00'")
    snapshotSpec(t)
  }

  test("allowed_expressions") {
    sql("""CREATE TABLE tbl (num INT, text STRING, d DOUBLE) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT c1 CHECK (num > 0)")
    sql("ALTER TABLE tbl ADD CONSTRAINT c2 CHECK (length(text) <= 10)")
    sql("ALTER TABLE tbl ADD CONSTRAINT c3 CHECK (d >= 0.0)")
    sql("INSERT INTO tbl VALUES (1, 'short', 1.5)")
    sql("INSERT INTO tbl VALUES (5, 'hello', 3.14)")
    sql("INSERT INTO tbl VALUES (10, 'world', 0.0)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "num > 7")
    snapshotSpec(t)
  }

  test("column_mapping") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive_id CHECK (id > 0)")
    sql("INSERT INTO tbl VALUES (1, 'a'),(2, 'b'),(3, 'c')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value = 'a'")
    snapshotSpec(t)
  }

  test("drop_feature") {
    sql("""CREATE TABLE tbl (x INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive CHECK (x > 0)")
    sql("INSERT INTO tbl VALUES (4),(5)")
    sql("ALTER TABLE tbl DROP CONSTRAINT positive")
    sql("INSERT INTO tbl VALUES (-1),(0)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 3)
    val N = 5L
    for (v <- 0L to N) snapshotSpec(t, version = v)
  }

  test("boolean_column_names") {
    sql("""CREATE TABLE tbl (id INT, flag BOOLEAN) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT flag_required CHECK (flag IS NOT NULL)")
    sql("INSERT INTO tbl VALUES (1, true),(2, false)")
    sql("INSERT INTO tbl VALUES (3, true),(4, true)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "flag = true")
    snapshotSpec(t)
  }

  test("decimal_constraint") {
    sql("""CREATE TABLE tbl (id INT, price DECIMAL(10,2), quantity INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive_price CHECK (price > 0)")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive_qty CHECK (quantity > 0)")
    sql("INSERT INTO tbl VALUES (1, 9.99, 5)")
    sql("INSERT INTO tbl VALUES (2, 49.99, 2)")
    sql("INSERT INTO tbl VALUES (3, 99.99, 1)")
    sql("INSERT INTO tbl VALUES (4, 149.99, 3)")
    sql("INSERT INTO tbl VALUES (5, 199.99, 10)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "price > 100")
    snapshotSpec(t)
  }

  test("complex_expr") {
    sql("""CREATE TABLE tbl (age INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT ck CHECK (age > 0 AND age < 200 OR name IS NOT NULL)")
    sql("INSERT INTO tbl VALUES (25, 'Alice'),(150, 'Bob')")
    sql("INSERT INTO tbl VALUES (30, 'Charlie')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "age > 100")
    snapshotSpec(t)
  }

  test("null_aware") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT ck_name CHECK (info.name IS NOT NULL)")
    sql("INSERT INTO tbl VALUES (1, named_struct('name', 'Alice', 'age', 25))")
    sql("INSERT INTO tbl VALUES (2, named_struct('name', 'Bob', 'age', 30))")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "info.age > 28")
    snapshotSpec(t)
  }

}
