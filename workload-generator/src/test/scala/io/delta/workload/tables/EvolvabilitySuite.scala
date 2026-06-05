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
 * Evolvability + Format Compatibility workloads (ev_* + fc_* family).
 * Covers: unknown action types in commit, unknown fields in add/metadata/protocol,
 * unknown protocol features, schema evolution across versions, partition null values,
 * empty JSON lines, extra metadata keys, forward compatibility.
 *
 */
class EvolvabilitySuite extends WorkloadTestSuite("evolvability") {

  // Evolvability: basic reads with unknown actions

  test("batch_read") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    // Inject unknown action type alongside valid data
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      java.nio.file.Files.write(f,
        (content.trim + "\n" + """{"unknownAction":{"key":"val"}}""" + "\n").getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("unknown_action_type") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      java.nio.file.Files.write(f,
        (content.trim + "\n" + """{"unknownAction":{"key":"value"}}""" + "\n").getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("protocol") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    snapshotSpec(t)
  }

  test("extra_protocol_fields") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    // Add unknown fields to protocol action
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"protocol\"")) {
          line.replace("\"protocol\":{",
            "\"protocol\":{\"futureField\":\"futureValue\",\"anotherUnknown\":42,")
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("unknown_protocol_feature") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    // Add unknown writer feature — should NOT block read
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"writerFeatures\"")) {
          line.replace("\"writerFeatures\":[",
            "\"writerFeatures\":[\"unknownFutureWriterFeature\",")
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    snapshotSpec(t)
  }

  test("unknown_writer_feature") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"writerFeatures\"")) {
          line.replace("\"writerFeatures\":[",
            "\"writerFeatures\":[\"unknownFutureWriterFeature\",")
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("unknown_reader_feature") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    // Add unknown reader feature — should block read
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"readerFeatures\"")) {
          line.replace("\"readerFeatures\":[",
            "\"readerFeatures\":[\"unknownFutureReaderFeature\",")
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("schema_evolution") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("ALTER TABLE tbl ADD COLUMN name STRING")
    sql("INSERT INTO tbl VALUES (5, 'alice'), (6, 'bob')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "name IS NOT NULL")
    snapshotSpec(t)
  }

  test("data_types") {
    sql("""CREATE TABLE tbl (
      id LONG, name STRING, score DOUBLE, active BOOLEAN, created DATE
    ) USING delta""")
    sql("INSERT INTO tbl VALUES (1, 'alice', 95.5, true, DATE'2024-01-01')")
    sql("INSERT INTO tbl VALUES (2, 'bob', 82.3, false, DATE'2024-06-15')")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("partitioned") {
    sql("CREATE TABLE tbl (id LONG, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "part = 'a'")
    snapshotSpec(t)
  }

  test("partition_null") {
    sql("CREATE TABLE tbl (id LONG, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, NULL), (3, 'b')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "part IS NULL")
    snapshotSpec(t)
  }

  test("future_commit_info") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"commitInfo\"")) {
          line.replace("\"commitInfo\":{",
            "\"commitInfo\":{\"futureCommitField\":\"someValue\",\"futureNestedField\":{\"x\":1},")
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("missing_intermediate_version") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    // Create 5 versions (0-4) with 2 files each
    sql("INSERT INTO tbl SELECT id FROM range(0, 10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    sql("INSERT INTO tbl SELECT id FROM range(30, 40)")
    // Force checkpoint at version 4
    forceCheckpoint("tbl")
    sql("INSERT INTO tbl SELECT id FROM range(40, 50)")
    val t = registerTable("tbl")
    // Delete intermediate versions (checkpoint covers them)
    mutateTable(t) { dir =>
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000001.json"))
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000002.json"))
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000003.json"))
    }
    readSpec(t)
    readSpec(t, predicate = "id >= 40")
    snapshotSpec(t)
  }

  test("fc_unknown_field_in_add") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"add\"")) {
          val node = mapper.readTree(line)
          val addNode = node.get("add").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
          addNode.put("unknownField", "testValue")
          mapper.writeValueAsString(node)
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("fc_unknown_field_in_metadata") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"metaData\"")) {
          line.replace("\"metaData\":{",
            "\"metaData\":{\"unknownMetadataField\":\"testValue\",")
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("fc_unknown_field_in_protocol") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"protocol\"")) {
          line.replace("\"protocol\":{",
            "\"protocol\":{\"unknownProtocolField\":\"testValue\",")
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("fc_unknown_action_top_level") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      java.nio.file.Files.write(f,
        (content.trim + "\n" + """{"futureAction":{"data":"test","version":99}}""" + "\n").getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("fc_null_fields_in_add") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"add\"")) {
          val node = mapper.readTree(line)
          val addNode = node.get("add").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
          addNode.putNull("nullField")
          addNode.putNull("anotherNullField")
          mapper.writeValueAsString(node)
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    readSpec(t)
    snapshotSpec(t)
  }

  test("fc_empty_json_line") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      // Insert blank lines between actions
      val withBlanks = content.split("\n").flatMap(line => Seq(line, "")).mkString("\n")
      java.nio.file.Files.write(f, withBlanks.getBytes)
    }
    readSpec(t)
    snapshotSpec(t)
  }

}
