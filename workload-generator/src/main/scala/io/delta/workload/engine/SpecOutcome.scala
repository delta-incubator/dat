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

import scala.util.control.NonFatal

import io.delta.workload.model._

// =============================================================================
// Spec outcome machinery
//
// The success/failure outcome concern shared by read and snapshot capture/validation: run a capture
// body to a [[SpecExpectation]], assert a declared [[ErrorExpectation]], compare a captured
// expectation against a re-run, and derive/normalize engine error codes.
// =============================================================================

object SpecOutcome {

  /**
   * The success/error verdict shared by read and snapshot validation. A `Failed` expectation must
   * reproduce as a failure (`run` throwing); the specific error code stays informational because it
   * varies across engines. A `Succeeded` expectation hands its expected result to `compare`, which
   * runs the operation and checks it.
   */
  def compareExpectation[R](expected: SpecExpectation[R], specName: String)(
      runError: => Option[String])(compare: R => Unit): Unit = expected match {
    case Failed(exp) =>
      // `runError` re-runs the operation and yields the actual error code (None on success),
      // derived the same way capture did, so codes compare cleanly. Capture and validation both
      // run with Spark, so they share an error-code vocabulary.
      val actual = runError
      require(actual.isDefined,
        s"Error validation FAILED for $specName: expected operation to fail " +
          s"(${exp.errorCode}) but it succeeded")
      require(normalizeErrorCode(actual.get) == normalizeErrorCode(exp.errorCode),
        s"Error validation FAILED for $specName: expected error code ${exp.errorCode} " +
          s"but got ${actual.get}")
    case Succeeded(exp) => compare(exp)
  }

  /**
   * Run a capture body to its [[SpecExpectation]]. The body returns the outcome directly (so a
   * capture can record a domain failure such as table-not-found); a non-fatal exception thrown
   * while capturing is recorded as a [[Failed]] with its extracted code. Truly-fatal throwables
   * (OOM, stack overflow, linkage/control-flow) propagate rather than masquerade as a Failed spec.
   */
  def captureExpectation[R](body: => SpecExpectation[R]): SpecExpectation[R] =
    try body
    catch {
      case NonFatal(e) => Failed(SpecError(extractErrorCode(e), Option(e.getMessage).getOrElse("")))
    }

  /**
   * Assert the declared [[ErrorExpectation]] against a captured `expectation`. `onMismatch` runs
   * before the failure is thrown (capture uses it to delete a half-written expected_data dir).
   */
  def assertExpectation[R](expectation: SpecExpectation[R], specName: String,
      expectError: ErrorExpectation)(onMismatch: => Unit = ()): Unit = {
    def fail(msg: String): Nothing = { onMismatch; throw new RuntimeException(s"$specName: $msg") }
    expectError match {
      case AutoDetect => // record whatever happened
      case AnyError => expectation match {
        case Succeeded(_) => fail("declared expectError=(any) but operation succeeded")
        case Failed(_) => // matches
      }
      case ErrorCode(code) => expectation match {
        case Succeeded(_) => fail(s"declared expectError='$code' but operation succeeded")
        case Failed(err) if normalizeErrorCode(err.errorCode) != normalizeErrorCode(code) =>
          fail(s"declared expectError='$code' but got '${err.errorCode}'")
        case Failed(_) => // matches
      }
    }
  }

  /** Re-run an operation purely to capture its error code (None = it succeeded). Non-fatal only. */
  def runErrorCode(body: => Option[String]): Option[String] =
    try body catch { case NonFatal(e) => Some(extractErrorCode(e)) }

  def extractErrorCode(e: Throwable): String = e match {
    case st: org.apache.spark.SparkThrowable =>
      Option(st.getErrorClass).getOrElse(e.getClass.getSimpleName)
    case _ => e.getClass.getSimpleName
  }

  def normalizeErrorCode(code: String): String = code match {
    case "DeltaIllegalStateException" => "DELTA_STATE_RECOVER_ERROR"
    // Version not found errors
    case "IllegalStateException" | "DELTA_LOG_FILE_NOT_FOUND" => "DELTA_VERSION_NOT_FOUND"
    // Timestamp invalid errors
    case "IllegalArgumentException" => "DELTA_TIMESTAMP_INVALID"
    case other => other
  }
}
