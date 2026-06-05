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

package io.delta.workload.log

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import io.delta.workload.JsonUtil
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class CommitLogSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var tableDir: Path = _

  override def beforeAll(): Unit = {
    tableDir = Files.createTempDirectory("commitlog-test-")
    Files.createDirectories(tableDir.resolve("_delta_log"))
  }

  override def afterAll(): Unit = {
    if (tableDir != null) FileUtils.deleteDirectory(tableDir.toFile)
  }

  // Canonical lines from a real commit 0 (CREATE TABLE tbl(id INT, value STRING))
  private val commitInfoLine =
    """{"commitInfo":{"timestamp":1776445636879,"operation":"CREATE TABLE","txnId":"abc"}}"""
  private val metadataLine =
    """{"metaData":{"id":"df7a6aaa","format":{"provider":"parquet","options":{}},""" +
      """"schemaString":"{}","partitionColumns":[],"configuration":{},"createdTime":1776445636863}}"""
  private val protocolLine =
    """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"""

  test("parse: commitInfo → RawAction (we don't type this)") {
    Action.parse(commitInfoLine) match {
      case RawAction(raw) => assert(raw == commitInfoLine)
      case other => fail(s"expected RawAction, got $other")
    }
  }

  test("parse: metaData → Metadata") {
    Action.parse(metadataLine) match {
      case m: Metadata =>
        assert(m.id == "df7a6aaa")
        assert(m.format == Format("parquet", Map.empty))
        assert(m.schemaString == "{}")
        assert(m.partitionColumns.isEmpty)
        assert(m.createdTime == Some(1776445636863L))
      case other => fail(s"expected Metadata, got $other")
    }
  }

  test("parse: protocol → Protocol") {
    Action.parse(protocolLine) match {
      case p: Protocol =>
        assert(p.minReaderVersion == 1)
        assert(p.minWriterVersion == 2)
        assert(p.readerFeatures.isEmpty)
        assert(p.writerFeatures.isEmpty)
      case other => fail(s"expected Protocol, got $other")
    }
  }

  test("parse: malformed → RawAction") {
    val bogus = """{"garbage":"not even an object"}"""
    Action.parse(bogus) match {
      case RawAction(raw) => assert(raw == bogus)
      case other => fail(s"expected RawAction, got $other")
    }
  }

  test("round-trip: Protocol") {
    val p = Protocol(3, 7, readerFeatures = Some(Set("deletionVectors")),
      writerFeatures = Some(Set("appendOnly", "deletionVectors")))
    val reparsed = Action.parse(p.toJson)
    assert(reparsed == p)
  }

  test("round-trip: Metadata with config and partitions") {
    val m = Metadata(id = "xyz", name = Some("t"), schemaString = "{}",
      partitionColumns = Seq("p1", "p2"),
      configuration = Map("delta.enableDeletionVectors" -> "true"),
      createdTime = Some(123L))
    val reparsed = Action.parse(m.toJson)
    assert(reparsed == m)
  }

  test("round-trip: AddFile with DeletionVector") {
    val dv = DeletionVector("u", "deadbeef", Some(0), 128, 7L)
    val a = AddFile(path = "part-0.parquet", size = 1024L,
      partitionValues = Map("p" -> "A"), modificationTime = 999L,
      stats = Some("""{"numRecords":10}"""), deletionVector = Some(dv))
    val reparsed = Action.parse(a.toJson)
    assert(reparsed == a)
  }

  test("round-trip: RemoveFile minimal") {
    val r = RemoveFile(path = "part-0.parquet", dataChange = false,
      deletionTimestamp = Some(42L))
    val reparsed = Action.parse(r.toJson)
    assert(reparsed == r)
  }

  test("CommitLog: read/write/mutate round-trip on commit file") {
    val lines = Seq(commitInfoLine, metadataLine, protocolLine)
    CommitLog.writeRaw(tableDir, 0L, lines)

    // Raw read preserves exactly
    assert(CommitLog.readRaw(tableDir, 0L) == lines)

    // Typed read parses into expected variants
    val actions = CommitLog.read(tableDir, 0L)
    assert(actions.size == 3)
    assert(actions(0).isInstanceOf[RawAction])     // commitInfo
    assert(actions(1).isInstanceOf[Metadata])
    assert(actions(2).isInstanceOf[Protocol])

    // Mutate: drop metadata, bump protocol
    CommitLog.mutate(tableDir, 0L) { actions =>
      actions.collect {
        case r: RawAction => r
        case p: Protocol => p.copy(minReaderVersion = 3, minWriterVersion = 7)
        // drops any Metadata
      }
    }

    val after = CommitLog.read(tableDir, 0L)
    assert(after.size == 2)
    assert(after.count(_.isInstanceOf[Metadata]) == 0)
    assert(after.last.asInstanceOf[Protocol].minReaderVersion == 3)
  }

  test("CommitLog: inject a malformed action via RawAction") {
    CommitLog.writeRaw(tableDir, 1L, Seq(protocolLine))
    CommitLog.mutate(tableDir, 1L) { _ :+ RawAction("""{"mystery":"field"}""") }
    val reread = CommitLog.readRaw(tableDir, 1L)
    assert(reread.size == 2)
    assert(reread.last == """{"mystery":"field"}""")
  }
}
