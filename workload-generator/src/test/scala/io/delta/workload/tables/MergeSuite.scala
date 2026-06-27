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
 * Merge operation workloads covering all MERGE INTO scenarios.
 *
 * Categories:
 *   - Basic clause types (insert, update, delete, combinations)
 *   - Conditional clauses
 *   - Data types (boolean, decimal, timestamp, string keys)
 *   - Complex types (array, map, nested struct)
 *   - Partitioned tables
 *   - Deletion vectors (DV-enabled tables with prior deletes)
 *   - Low-shuffle merge variants
 *   - Schema evolution (add columns, nested fields, type widening, column mapping)
 *   - Struct evolution (deep nesting, null handling, arrays of structs, maps of structs)
 *   - Edge cases (empty source/target, null join keys, self-merge, duplicate source, aliases)
 *   - Error cases (ambiguous column, type mismatch, no match condition)
 *   - NOT MATCHED BY SOURCE clauses
 *   - CDF-enabled merge
 *
 */
class MergeSuite extends WorkloadTestSuite("merge") {

  // Basic clause types

  test("basic_insert") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(4,'d'),(5,'e') AS s(id, value)) s
      ON t.id = s.id
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id >= 4", name = Some("filter_new_rows"))
    snapshotSpec(t)
  }

  test("basic_update") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id = 2", name = Some("filter_updated"))
    snapshotSpec(t)
  }

  test("basic_delete") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN DELETE""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("insert_update") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(3,'c') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id = 2", name = Some("filter_id_2"))
    snapshotSpec(t)
  }

  test("insert_delete") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(4,'d') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN DELETE
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("insert_update_delete") {
    sql("""CREATE TABLE tbl (id INT, val INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,10),(2,20),(3,30)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,40),(4,200) AS s(id, val)) s
      ON t.id = s.id
      WHEN MATCHED AND t.val > 15 THEN UPDATE SET val = s.val
      WHEN MATCHED THEN DELETE
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("update_delete") {
    sql("""CREATE TABLE tbl (id INT, val INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,10),(2,20),(3,30)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,100),(2,200),(3,300) AS s(id, val)) s
      ON t.id = s.id
      WHEN MATCHED AND t.val > 15 THEN UPDATE SET val = s.val
      WHEN MATCHED THEN DELETE""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("multiple_matched") {
    sql("""CREATE TABLE tbl (id INT, score INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,5),(2,15),(3,35)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,50),(2,50),(3,50) AS s(id, score)) s
      ON t.id = s.id
      WHEN MATCHED AND t.score > 30 THEN DELETE
      WHEN MATCHED AND t.score > 10 THEN UPDATE SET score = s.score""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("conditional_insert") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(3,'c'),(4,'d') AS s(id, value)) s
      ON t.id = s.id
      WHEN NOT MATCHED AND s.id > 3 THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("conditional_update") {
    sql("""CREATE TABLE tbl (id INT, status STRING, score INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'low',5),(2,'low',15),(3,'low',25)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'high',50),(3,'high',50) AS s(id, status, score)) s
      ON t.id = s.id
      WHEN MATCHED AND t.score >= 10 THEN UPDATE SET status = s.status, score = s.score""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "status = 'high'", name = Some("filter_high"))
    snapshotSpec(t)
  }

  test("star_insert") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (3,'c'),(4,'d') AS s(id, value)) s
      ON t.id = s.id
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("star_update") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("source_subquery") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t
      USING (SELECT id, CONCAT(value, '_new') AS value FROM VALUES (2,'x'),(3,'c') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("source_aggregation") {
    sql("""CREATE TABLE tbl (id INT, total BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,100),(2,200)")
    sql("""MERGE INTO tbl t
      USING (SELECT id, SUM(amount) AS total FROM VALUES (2,30),(2,50),(3,150) AS s(id, amount) GROUP BY id) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET total = s.total
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("boolean_values") {
    sql("""CREATE TABLE tbl (id INT, active BOOLEAN) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,true),(2,false),(3,true)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,true),(4,false) AS s(id, active)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET active = s.active
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "active = true", name = Some("filter_active"))
    snapshotSpec(t)
  }

  test("decimal_values") {
    sql("""CREATE TABLE tbl (id INT, price DECIMAL(10,2)) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,10.50),(2,20.75)")
    sql("INSERT INTO tbl VALUES (3,30.00)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,99.99),(4,45.00) AS s(id, price)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET price = s.price
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "price > 30.00", name = Some("filter_price"))
    snapshotSpec(t)
  }

  test("timestamp_values") {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, TIMESTAMP '2024-01-01 10:00:00')")
    sql("INSERT INTO tbl VALUES (2, TIMESTAMP '2024-01-02 10:00:00')")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, TIMESTAMP '2024-06-01 12:00:00'),(3, TIMESTAMP '2024-07-01 14:00:00') AS s(id, ts)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET ts = s.ts
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("string_keys") {
    sql("""CREATE TABLE tbl (name STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES ('alice',100),('bob',200)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES ('bob',999),('carol',300) AS s(name, amount)) s
      ON t.name = s.name
      WHEN MATCHED THEN UPDATE SET amount = s.amount
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "name = 'bob'", name = Some("filter_bob"))
    snapshotSpec(t)
  }

  test("null_handling") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a')")
    sql("INSERT INTO tbl VALUES (2, NULL)")
    sql("INSERT INTO tbl VALUES (3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'updated'),(4,CAST(NULL AS STRING)) AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value IS NOT NULL", name = Some("filter_not_null"))
    snapshotSpec(t)
  }

  test("with_array_col") {
    sql("""CREATE TABLE tbl (id INT, tags ARRAY<STRING>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, array('a','b'))")
    sql("INSERT INTO tbl VALUES (2, array('c'))")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, array('c','d','e')),(3, array('f')) AS s(id, tags)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET tags = s.tags
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("with_map_col") {
    sql("""CREATE TABLE tbl (id INT, props MAP<STRING, STRING>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, map('k1','v1'))")
    sql("INSERT INTO tbl VALUES (2, map('k2','v2'))")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, map('k2','updated')),(3, map('k3','v3')) AS s(id, props)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET props = s.props
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("with_nested_struct") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25))")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES
        (2, named_struct('name','bob','age',26)),
        (3, named_struct('name','carol','age',35))
      AS s(id, info)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET info = s.info
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("partitioned_basic") {
    sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
      PARTITIONED BY (region)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'east',100),(2,'west',200),(3,'east',300)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'west',999),(4,'north',400) AS s(id, region, amount)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET amount = s.amount
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "region = 'east'", name = Some("filter_east"))
    snapshotSpec(t)
  }

  test("partitioned_cross_partition") {
    sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
      PARTITIONED BY (region)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'east',100),(2,'east',200),(3,'west',300)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'west',150),(2,'west',250) AS s(id, region, amount)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET region = s.region, amount = s.amount""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "region = 'west'", name = Some("filter_west"))
    snapshotSpec(t)
  }

  test("partitioned_multi_col") {
    sql("""CREATE TABLE tbl (id INT, country STRING, year INT, amount INT) USING delta
      PARTITIONED BY (country, year)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'US',2024,100),(2,'UK',2024,200),(3,'US',2023,300)")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (1,'US',2024,999),(4,'DE',2024,400) AS s(id, country, year, amount)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET amount = s.amount
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "country = 'US' AND year = 2024", name = Some("filter_us_2024"))
    snapshotSpec(t)
  }

  test("dv_basic_insert_update") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 2")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x'),(2,'x'),(4,'d') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t, name = Some("readAll"))
    snapshotSpec(t)
  }

  test("dv_conditional_update") {
    sql("""CREATE TABLE tbl (id INT, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,100),(2,200),(3,300),(4,400)")
    sql("DELETE FROM tbl WHERE id = 4")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,250),(3,350),(5,500) AS s(id, amount)) s
      ON t.id = s.id
      WHEN MATCHED AND s.amount > 200 THEN UPDATE SET amount = s.amount
      WHEN NOT MATCHED AND s.amount >= 500 THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t, name = Some("readAll"))
    snapshotSpec(t)
  }

  test("dv_delete_clause") {
    sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',40),(4,'d',50),(5,'e',60)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,99),(3,99),(4,99) AS s(id, amount)) s
      ON t.id = s.id
      WHEN MATCHED AND t.amount > 30 THEN DELETE
      WHEN MATCHED THEN UPDATE SET amount = s.amount""")
    val t = registerTable("tbl")
    readSpec(t, name = Some("readAll"))
    snapshotSpec(t)
  }

  test("dv_large_table") {
    sql("""CREATE TABLE tbl (id INT, name STRING, score INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, CONCAT('name_', CAST(id AS STRING)), id * 100 FROM range(100)")
    sql("DELETE FROM tbl WHERE id <= 30 AND id % 3 = 0")
    sql("""MERGE INTO tbl t USING (
      SELECT id, CONCAT('updated_', CAST(id AS STRING)) AS name, id * 1000 AS score
      FROM range(50, 60)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t, name = Some("readAll"))
    readSpec(t, predicate = "score > 5000", name = Some("filterHighScore"))
    snapshotSpec(t)
  }

  test("dv_multiple_matched") {
    sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40),(5,'e',50)")
    sql("DELETE FROM tbl WHERE id = 1")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,99),(3,99),(4,99) AS s(id, amount)) s
      ON t.id = s.id
      WHEN MATCHED AND t.amount > 100 THEN DELETE
      WHEN MATCHED AND t.amount > 50 THEN UPDATE SET value = 'also', amount = s.amount
      WHEN MATCHED THEN UPDATE SET value = 'updated', amount = s.amount""")
    val t = registerTable("tbl")
    readSpec(t, name = Some("readAll"))
    snapshotSpec(t)
  }

  test("dv_multiple_merges") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 2")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x'),(2,'new'),(4,'d') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    sql("DELETE FROM tbl WHERE id = 1")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'back'),(5,'e') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t, name = Some("readAll"))
    snapshotSpec(t)
  }

  test("dv_null_handling") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,NULL),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 1")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'updated'),(4,CAST(NULL AS STRING)) AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t, name = Some("readAll"))
    snapshotSpec(t)
  }

  test("dv_partitioned") {
    sql("""CREATE TABLE tbl (region STRING, id INT, value STRING) USING delta
      PARTITIONED BY (region)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES ('us',1,'a'),('us',2,'b'),('eu',3,'c'),('eu',4,'d')")
    sql("DELETE FROM tbl WHERE region = 'us' AND id = 1")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES ('us',2,'x'),('eu',5,'e') AS s(region, id, value)) s
      ON t.id = s.id AND t.region = s.region
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t, name = Some("readAll"))
    snapshotSpec(t)
  }

  test("dv_star_syntax") {
    sql("""CREATE TABLE tbl (id INT, name STRING, score INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30)")
    sql("DELETE FROM tbl WHERE id = 2")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x',99),(2,'new',50),(4,'d',40) AS s(id, name, score)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t, name = Some("readAll"))
    snapshotSpec(t)
  }

  test("low_shuffle_basic") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(4,'d') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id = 2", name = Some("filter_updated"))
    snapshotSpec(t)
  }

  test("low_shuffle_conditional") {
    sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',25),(3,'y',35),(5,'e',100) AS s(id, value, amount)) s
      ON t.id = s.id
      WHEN MATCHED AND s.amount > t.amount THEN UPDATE SET value = s.value, amount = s.amount
      WHEN NOT MATCHED AND s.amount > 50 THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id = 5", name = Some("filter_conditional_insert"))
    snapshotSpec(t)
  }

  test("low_shuffle_decimal") {
    sql("""CREATE TABLE tbl (id INT, price DECIMAL(10,2)) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,10.50),(2,20.75),(3,30.00)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,99.99),(4,45.00) AS s(id, price)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET price = s.price
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id = 2", name = Some("filter_updated"))
    snapshotSpec(t)
  }

  test("low_shuffle_large_table") {
    sql("""CREATE TABLE tbl (id BIGINT, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, CAST(id * 10 AS INT) FROM range(200)")
    sql("""MERGE INTO tbl t USING (
      SELECT id, 9999 AS value FROM range(100, 110)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value = 9999", name = Some("filter_updated"))
    snapshotSpec(t)
  }

  test("low_shuffle_multi_clause") {
    sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',0),(4,'d',40)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',25),(3,'y',35),(5,'e',50) AS s(id, value, amount)) s
      ON t.id = s.id
      WHEN MATCHED AND t.amount = 0 THEN DELETE
      WHEN MATCHED THEN UPDATE SET value = s.value, amount = s.amount
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id = 3", name = Some("filter_deleted"))
    snapshotSpec(t)
  }

  test("low_shuffle_multi_merge") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(3,'c') AS s(id, value)) s
      ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT *""")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (3,'y'),(4,'d') AS s(id, value)) s
      ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT *""")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (4,'z'),(5,'e') AS s(id, value)) s
      ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id = 5", name = Some("filter_last_merge"))
    snapshotSpec(t)
  }

  test("low_shuffle_nested") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30)),(2, named_struct('name','bob','age',25))")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, named_struct('name','bob','age',26)),(3, named_struct('name','carol','age',35)) AS s(id, info)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET info = s.info
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("low_shuffle_partitioned") {
    sql("""CREATE TABLE tbl (id INT, part STRING, amount INT) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'x',10),(2,'y',20),(3,'x',30)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'y',99),(4,'z',40) AS s(id, part, amount)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET amount = s.amount
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "part = 'y'", name = Some("filter_part_y"))
    snapshotSpec(t)
  }

  test("low_shuffle_star") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(3,'c') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("low_shuffle_timestamp") {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP, label STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, TIMESTAMP '2024-01-01 10:00:00', 'old1'),(2, TIMESTAMP '2024-01-02 10:00:00', 'old2')")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, TIMESTAMP '2024-06-01 12:00:00', 'upd'),(3, TIMESTAMP '2024-07-01 14:00:00', 'new') AS s(id, ts, label)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET ts = s.ts, label = s.label
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id = 3", name = Some("filter_new"))
    snapshotSpec(t)
  }

  test("edge_all_matched") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'z'),(2,'z'),(3,'z') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "value = 'z'", name = Some("filter_updated"))
    snapshotSpec(t)
  }

  test("edge_no_matched") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (10,'x'),(20,'y') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id >= 10", name = Some("filter_new"))
    snapshotSpec(t)
  }

  test("edge_empty_source") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (999,'x') AS s(id, value) WHERE false) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("edge_empty_target") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (999,'placeholder')")
    sql("DELETE FROM tbl WHERE true")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'a'),(2,'b') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("edge_null_join_key") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(NULL,'c')")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2,'updated'),(4,'new'),(CAST(NULL AS INT),'null_src') AS s(id, value)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id IS NULL", name = Some("filter_nulls"))
    snapshotSpec(t)
  }

  test("edge_self_merge") {
    sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30)")
    sql("""MERGE INTO tbl t USING tbl s
      ON t.id = s.id AND t.amount < 25
      WHEN MATCHED THEN UPDATE SET amount = s.amount * 2""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "amount = 20", name = Some("filter_doubled"))
    snapshotSpec(t)
  }

  test("edge_source_alias") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (
      SELECT id AS src_id, value AS src_value FROM VALUES (2,'x') AS s(id, value)
    ) s ON t.id = s.src_id
      WHEN MATCHED THEN UPDATE SET value = s.src_value""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id = 2", name = Some("filter_updated"))
    snapshotSpec(t)
  }

  test("edge_multi_join") {
    sql("""CREATE TABLE tbl (id INT, key STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'x',10),(2,'y',20),(3,'z',30)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'y',999),(4,'w',400) AS s(id, key, amount)) s
      ON t.id = s.id AND t.key = s.key
      WHEN MATCHED THEN UPDATE SET amount = s.amount
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id = 2 AND amount = 999", name = Some("filter_updated"))
    snapshotSpec(t)
  }

  test("edge_large_payload") {
    val cols = (1 to 20).map(i => s"col_$i INT").mkString(", ")
    sql(s"""CREATE TABLE tbl (id INT, $cols) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    val vals1 = (1 to 20).map(i => i * 10).mkString(",")
    val vals2 = (1 to 20).map(i => i * 20).mkString(",")
    sql(s"INSERT INTO tbl VALUES (1,$vals1),(2,$vals2)")
    val srcVals2 = (1 to 20).map(i => i * 99).mkString(",")
    val srcVals3 = (1 to 20).map(i => i * 30).mkString(",")
    sql(s"""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,$srcVals2),(3,$srcVals3) AS s(id,${(1 to 20).map(i => s"col_$i").mkString(",")})) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET ${(1 to 20).map(i => s"col_$i = s.col_$i").mkString(",")}
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("edge_duplicate_source_keys") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    // Merge with duplicate source keys should fail; table stays at version 0
    try {
      sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x'),(1,'y') AS s(id, value)) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value""")
    } catch { case _: Exception => /* expected failure */ }
    val t = registerTable("tbl")
    readSpec(t, name = Some("readAll"))
    snapshotSpec(t)
  }

  test("err_ambiguous_column") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    try {
      sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = value""")
    } catch { case _: Exception => /* expected: ambiguous column reference */ }
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("err_duplicate_source") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    try {
      sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x'),(1,'y') AS s(id, value)) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value""")
    } catch { case _: Exception => /* expected: duplicate source */ }
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("err_no_match_condition") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    // This should fail at parse time (no ON clause)
    try {
      sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x') AS s(id, value)) s
        WHEN MATCHED THEN UPDATE SET value = s.value""")
    } catch { case _: Exception => /* expected: parse error */ }
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("err_type_mismatch") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    try {
      sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1, ARRAY(1,2,3)) AS s(id, value)) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value""")
    } catch { case _: Exception => /* expected: type mismatch */ }
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("struct_evo_null_in_key") {
    sql("""CREATE TABLE tbl (key STRUCT<k1: INT, k2: STRING>, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (named_struct('k1',1,'k2','a'), 10),(named_struct('k1',2,'k2',CAST(NULL AS STRING)), 20)")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (named_struct('k1',1,'k2','a'), 99),(named_struct('k1',3,'k2','c'), 30) AS s(key, value)
    ) s ON t.key = s.key
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t, name = Some("readAll"))
    snapshotSpec(t)
  }

  test("by_source_all_clause_types") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(4,'d') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *
      WHEN NOT MATCHED BY SOURCE THEN DELETE""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("by_source_not_matched_by_source_delete") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED BY SOURCE THEN DELETE""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("by_source_not_matched_by_source_update") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED BY SOURCE THEN UPDATE SET value = 'orphan'""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("by_source_with_change_tracking") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(4,'d') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("by_source_with_dv") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED BY SOURCE THEN DELETE""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

}
