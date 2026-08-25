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

package io.delta.workload.engine

import org.scalatest.funsuite.AnyFunSuite

import io.delta.workload.model._

/** Unit tests for the spec-outcome machinery (no Spark): capture/assert/error-code helpers. */
class SpecOutcomeSuite extends AnyFunSuite {

  test("captureExpectation: returns the body's outcome (success or domain failure) verbatim") {
    assert(SpecOutcome.captureExpectation(Succeeded(ReadResult(1, 1, 0))) ==
      Succeeded(ReadResult(1, 1, 0)))
    val fail = Failed(SpecError("DELTA_TABLE_NOT_FOUND", "x"))
    assert(SpecOutcome.captureExpectation[ReadResult](fail) == fail)
  }

  test("captureExpectation: a non-fatal exception becomes Failed with its code + message") {
    val out = SpecOutcome.captureExpectation[ReadResult](throw new IllegalStateException("boom"))
    out match {
      case Failed(SpecError(code, msg)) =>
        assert(code == "IllegalStateException"); assert(msg == "boom")
      case other => fail(s"expected Failed, got $other")
    }
  }

  test("captureExpectation: a FATAL throwable propagates (not swallowed as Failed)") {
    intercept[OutOfMemoryError] {
      SpecOutcome.captureExpectation[ReadResult](throw new OutOfMemoryError("fatal"))
    }
  }

  test("assertExpectation: AutoDetect never fails, for success or failure") {
    SpecOutcome.assertExpectation(Succeeded(ReadResult(0, 0, 0)), "s", AutoDetect)()
    SpecOutcome.assertExpectation[ReadResult](Failed(SpecError("E", "m")), "s", AutoDetect)()
  }

  test("assertExpectation: AnyError requires a failure") {
    SpecOutcome.assertExpectation[ReadResult](Failed(SpecError("E", "m")), "s", AnyError)()
    intercept[RuntimeException] {
      SpecOutcome.assertExpectation(Succeeded(ReadResult(0, 0, 0)), "s", AnyError)()
    }
  }

  test("assertExpectation: ErrorCode matches on normalized code, fails on mismatch or success") {
    // exact match
    SpecOutcome.assertExpectation[ReadResult](
      Failed(SpecError("DELTA_TABLE_NOT_FOUND", "")), "s", ErrorCode("DELTA_TABLE_NOT_FOUND"))()
    // normalized match: IllegalStateException ~ DELTA_VERSION_NOT_FOUND
    SpecOutcome.assertExpectation[ReadResult](
      Failed(SpecError("IllegalStateException", "")), "s", ErrorCode("DELTA_VERSION_NOT_FOUND"))()
    intercept[RuntimeException] { // wrong code
      SpecOutcome.assertExpectation[ReadResult](
        Failed(SpecError("DELTA_OTHER", "")), "s", ErrorCode("DELTA_TABLE_NOT_FOUND"))()
    }
    intercept[RuntimeException] { // succeeded but error expected
      SpecOutcome.assertExpectation(Succeeded(ReadResult(0, 0, 0)), "s", ErrorCode("E"))()
    }
  }

  test("assertExpectation: onMismatch runs before the failure is thrown") {
    var cleaned = false
    intercept[RuntimeException] {
      SpecOutcome.assertExpectation(Succeeded(ReadResult(0, 0, 0)), "s", AnyError) { cleaned = true }
    }
    assert(cleaned, "onMismatch hook must run before throwing")
  }

  test("runErrorCode: passes through a domain code / None, and catches non-fatal as a code") {
    assert(SpecOutcome.runErrorCode(None).isEmpty)
    assert(SpecOutcome.runErrorCode(Some("DELTA_TABLE_NOT_FOUND")).contains("DELTA_TABLE_NOT_FOUND"))
    assert(SpecOutcome.runErrorCode(throw new IllegalArgumentException()).contains("IllegalArgumentException"))
  }

  test("normalizeErrorCode: known aliases map to canonical DELTA_* codes, others pass through") {
    assert(SpecOutcome.normalizeErrorCode("IllegalStateException") == "DELTA_VERSION_NOT_FOUND")
    assert(SpecOutcome.normalizeErrorCode("DELTA_LOG_FILE_NOT_FOUND") == "DELTA_VERSION_NOT_FOUND")
    assert(SpecOutcome.normalizeErrorCode("IllegalArgumentException") == "DELTA_TIMESTAMP_INVALID")
    assert(SpecOutcome.normalizeErrorCode("DeltaIllegalStateException") == "DELTA_STATE_RECOVER_ERROR")
    assert(SpecOutcome.normalizeErrorCode("DELTA_TABLE_NOT_FOUND") == "DELTA_TABLE_NOT_FOUND")
  }

  test("extractErrorCode: a plain throwable yields its simple class name") {
    assert(SpecOutcome.extractErrorCode(new RuntimeException("x")) == "RuntimeException")
  }
}
