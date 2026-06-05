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

class DefaultValuesSuite extends WorkloadTestSuite("default_values") {

  test("read_with_defaults") {
    sql("""CREATE TABLE tbl (id INT, name STRING DEFAULT 'unknown', score DOUBLE DEFAULT 0.0)
      USING delta TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    sql("INSERT INTO tbl(id) VALUES (1),(2)")
    sql("INSERT INTO tbl VALUES (3, 'alice', 95.0)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "name = 'unknown'")
    snapshotSpec(t)
  }

  test("read_default_types") {
    sql("""CREATE TABLE tbl (
      id INT, int_col INT DEFAULT 42, str_col STRING DEFAULT 'hello',
      bool_col BOOLEAN DEFAULT true, double_col DOUBLE DEFAULT 3.14
    ) USING delta TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    sql("INSERT INTO tbl(id) VALUES (1),(2)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("read_default_nested") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>)
      USING delta TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    sql("INSERT INTO tbl VALUES (2, NULL)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "info IS NOT NULL")
    snapshotSpec(t)
  }

}
