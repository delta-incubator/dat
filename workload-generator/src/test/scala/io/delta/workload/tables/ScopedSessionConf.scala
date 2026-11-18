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

import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach

import io.delta.workload.WorkloadTestSuite

/**
 * Session settings that a single workload test needs while its specs are captured.
 *
 * `WorkloadTestSuite` generates and asserts specs after the test body returns, so a
 * try/finally inside the body would restore a setting before capture. A setting applied
 * with [[setForTest]] stays active through generation and spec assertions, and is restored
 * after the suite's per-test cleanup, including when the body, generation or an assertion
 * fails. Other tests in the suite keep the session's original settings.
 */
trait ScopedSessionConf extends BeforeAndAfterEach { this: WorkloadTestSuite =>

  // Key -> (session, value before this test first set it).
  private val savedConf = mutable.LinkedHashMap.empty[String, (SparkSession, Option[String])]

  protected def setForTest(key: String, value: String): Unit = {
    val session = spark
    if (!savedConf.contains(key)) savedConf(key) = session -> session.conf.getOption(key)
    session.conf.set(key, value)
  }

  override def afterEach(): Unit = {
    try super.afterEach() finally {
      savedConf.toSeq.reverse.foreach {
        case (key, (session, Some(previous))) => session.conf.set(key, previous)
        case (key, (session, None)) => session.conf.unset(key)
      }
      savedConf.clear()
    }
  }
}
