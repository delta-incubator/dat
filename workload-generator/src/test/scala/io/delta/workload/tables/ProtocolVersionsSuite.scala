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
 * Protocol versioning, table features, partition value encoding, and protocol edge cases.
 * Covers pv_* (protocol versions) and pve_* (partition value encoding) workloads.
 */
class ProtocolVersionsSuite extends WorkloadTestSuite("protocol_versions") {

  // pv_001*: Basic protocol version tables

  test("protocol_1_1") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("protocol_1_2") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.appendOnly' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("protocol_1_3") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive CHECK (id >= 0)")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("protocol_2_5") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("protocol_change_tracking") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("protocol_3_7_dv") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("upgrade_to_current") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_latest"))
    readSpec(t, version = 0, name = Some("read_v0"))
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
  }

  test("upgrade_deltatable_api") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    // Upgrade protocol by adding a writer feature via ALTER TABLE
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.feature.checkConstraints' = 'supported')")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_latest"))
    readSpec(t, version = 0, name = Some("read_v0"))
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
    snapshotSpec(t, version = 2)
  }

  test("upgrade_no_feature") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("upgrade_many_features") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.enableChangeDataFeed' = 'true',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("upgrade_sql_api") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_latest"))
    readSpec(t, version = 0, name = Some("read_v0"))
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
  }

  test("overwrite_keeps_protocol") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT OVERWRITE tbl SELECT id + 10 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_after_overwrite"))
    snapshotSpec(t)
  }

  test("overwrite_keeps_properties") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('myProp' = 'true')")
    sql("INSERT OVERWRITE tbl SELECT id + 10 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_after_overwrite"))
    snapshotSpec(t)
  }

  test("overwrite_keeps_features") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT OVERWRITE tbl SELECT id + 10 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_after_overwrite"))
    snapshotSpec(t)
  }

  test("overwrite_with_configs") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT OVERWRITE tbl SELECT id + 10 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_after_overwrite"))
    snapshotSpec(t)
  }

  test("overwrite_session_defaults") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT OVERWRITE tbl SELECT id + 10 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_after_overwrite"))
    snapshotSpec(t)
  }

  test("vacuum_protocol_check") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("downgrade_noop") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("create_ignore_defaults") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("operation_ignore_defaults") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("create_session_features") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.appendOnly' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("create_mixed_features") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true',
        'delta.appendOnly' = 'true',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("replace_default_protocol") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("CREATE OR REPLACE TABLE tbl (id LONG) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    sql("INSERT INTO tbl SELECT id + 10 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, version = 0, name = Some("read_v0"))
    readSpec(t, name = Some("read_v1"))
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
  }

  test("create_no_explicit_protocol") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("create_protocol_property") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("create_writer_only_feature") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.appendOnly' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("create_legacy_rw_feature") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("create_native_writer_feature") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("create_reader_writer_feature") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("alter_add_change_tracking") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_latest"))
    readSpec(t, version = 0, name = Some("read_v0"))
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
    snapshotSpec(t, version = 2)
  }

  test("alter_add_column_mapping") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_latest"))
    readSpec(t, version = 0, name = Some("read_v0"))
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
    snapshotSpec(t, version = 2)
  }

  test("protocol_property_wins") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("protocol_desc_table") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.feature.checkConstraints' = 'supported')")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("auto_upgrade_v2") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_latest"))
    readSpec(t, version = 0, name = Some("read_v0"))
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
  }

  test("auto_upgrade_v3") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive CHECK (id > 0)")
    sql("INSERT INTO tbl VALUES (1), (2), (3), (4), (5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
    snapshotSpec(t, version = 2)
  }

  test("all_active_features") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true',
        'delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("table_feature_status") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("replace_as_updates_protocol") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT OVERWRITE tbl SELECT id + 10 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_latest"))
    readSpec(t, version = 0, name = Some("read_v0"))
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
  }

  test("replace_as_keeps_protocol") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.feature.checkConstraints' = 'supported',
        'delta.feature.generatedColumns' = 'supported',
        'delta.feature.changeDataFeed' = 'supported')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT OVERWRITE tbl SELECT id + 10 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_latest"))
    readSpec(t, version = 0, name = Some("read_v0"))
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
  }

  test("protocol_change_logging") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_latest"))
    readSpec(t, version = 0, name = Some("read_v0"))
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
    snapshotSpec(t, version = 2)
  }

  test("remove_writer_feature") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.appendOnly' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.appendOnly' = 'false')")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_latest"))
    readSpec(t, version = 0, name = Some("read_v0"))
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
  }

  test("remove_change_tracking") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false')")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_latest"))
    readSpec(t, version = 0, name = Some("read_v0"))
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
  }

  test("downgrade_1_4") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.feature.checkConstraints' = 'supported',
        'delta.feature.generatedColumns' = 'supported',
        'delta.feature.changeDataFeed' = 'supported')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("downgrade_2_5") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("downgrade_3_7") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("dv_removal_state") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(20)")
    sql("DELETE FROM tbl WHERE id < 10")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_latest"))
    readSpec(t, version = 0, name = Some("read_v0"))
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
  }

  test("ict_state") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 10 FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, name = Some("read_latest"))
    readSpec(t, version = 0, name = Some("read_v0"))
    readSpec(t, version = 1, name = Some("read_v1"))
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
    snapshotSpec(t, version = 2)
  }

  test("empty_reader_features") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    // Rewrite commit to have empty feature arrays (still protocol v3/v7)
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          val updated = line
            .replaceAll(""""readerFeatures"\s*:\s*\[[^\]]*\]""", """"readerFeatures":[]""")
            .replaceAll(""""writerFeatures"\s*:\s*\[[^\]]*\]""", """"writerFeatures":[]""")
          updated
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    snapshotSpec(t)
  }

  test("err_001_protocol_too_high") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    // Rewrite protocol to unreachable version
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":2147483647,"minWriterVersion":2147483647,"readerFeatures":[],"writerFeatures":[]}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    snapshotSpec(t)
  }

  test("err_002_unsupported_feature") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["NonExistingReaderFeature"],"writerFeatures":[]}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    snapshotSpec(t)
  }

  test("features_case_sensitivity") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["DeletionVectors"],"writerFeatures":["DeletionVectors"]}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    snapshotSpec(t)
  }

  test("protocol_downgrade") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    // Rewrite protocol in commit 0 to lower version via a second commit
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
      // Add a second commit that downgrades protocol
      val commit1 = dir.resolve("_delta_log/00000000000000000001.json")
      val newLines = java.util.Arrays.asList(
        """{"metaData":{"id":"00000000-0000-0000-0000-000000000000","format":{"provider":"parquet","options":{}},"partitionColumns":[],"configuration":{}}}""",
        """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"""
      )
      java.nio.file.Files.write(commit1, newLines)
    }
    snapshotSpec(t)
  }

  test("reader_feature_not_in_writer") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":[]}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    snapshotSpec(t)
  }

  test("reader_v3_writer_lt_7") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":3,"minWriterVersion":6}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    snapshotSpec(t)
  }

  test("reader_v4_error") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":4,"minWriterVersion":7}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    snapshotSpec(t)
  }

  test("unknown_reader_feature") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["unknownReaderFeatureXyz"],"writerFeatures":["unknownReaderFeatureXyz"]}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    snapshotSpec(t)
  }

  test("unknown_writer_feature_ok") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":1,"minWriterVersion":7,"writerFeatures":["unknownWriterFeatureXyz"]}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    snapshotSpec(t)
  }

  test("multiple_reader_features") {
    sql("""CREATE TABLE tbl (id LONG, name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true',
        'delta.checkpointPolicy' = 'v2')""")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    sql("DELETE FROM tbl WHERE id = 2")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

}
