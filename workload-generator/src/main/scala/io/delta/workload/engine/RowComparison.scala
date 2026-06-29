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

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StringType, StructType}

// =============================================================================
// Row equality
//
// The row-equality concern: assert two DataFrames hold the same bag of rows, type-exactly, after
// canonicalizing maps/variants into set-op-able forms.
// =============================================================================

object RowComparison {

  /**
   * Assert `expected` and `actual` hold the same bag of rows, type-exactly. Column names and types
   * must match (top-level nullability ignored; `catalogString` carries decimal scale, timestamp_ntz,
   * and nested field types) so Int and Long never compare equal and decimal scale is preserved. Rows
   * are diffed with Spark `exceptAll` (bag semantics, so duplicates count) after [[canonicalizeRows]]
   * projects maps and variant into set-op-able forms.
   */
  def assertRowsEqual(expected: DataFrame, actual: DataFrame, specName: String): Unit = {
    val expTypes = expected.schema.map(f => f.name -> f.dataType.catalogString)
    val actTypes = actual.schema.map(f => f.name -> f.dataType.catalogString)
    // The type match also guarantees the canonicalized frames share a schema, so exceptAll below
    // cannot raise an analysis error on mismatched columns.
    require(expTypes == actTypes,
      s"Validation FAILED for $specName: schema mismatch" +
        s"\n  expected: ${expTypes.mkString(", ")}\n  actual:   ${actTypes.mkString(", ")}")
    val e = canonicalizeRows(expected)
    val a = canonicalizeRows(actual)
    val missing = e.exceptAll(a)
    val extra = a.exceptAll(e)
    val nMissing = missing.count()
    val nExtra = extra.count()
    if (nMissing != 0 || nExtra != 0) {
      val details = new StringBuilder()
      if (nMissing != 0) {
        details.append(s"\n  Missing rows: $nMissing")
        missing.take(3).foreach(r => details.append(s"\n    $r"))
      }
      if (nExtra != 0) {
        details.append(s"\n  Extra rows: $nExtra")
        extra.take(3).foreach(r => details.append(s"\n    $r"))
      }
      throw new RuntimeException(s"Validation FAILED for $specName: row-level mismatch$details")
    }
  }

  /**
   * Project every column into a type Spark set operations accept: `exceptAll` rejects map and
   * variant columns, so maps become sorted key/value entry arrays (order-insensitive) and variant
   * becomes its JSON string; structs and arrays recurse.
   */
  private def canonicalizeRows(df: DataFrame): DataFrame =
    df.select(df.schema.fields.map(f =>
      canonicalizeCol(SnapshotResolver.columnRef(f.name), f.dataType).as(f.name)): _*)

  private def canonicalizeCol(c: Column, dt: DataType): Column = dt match {
    case mt: MapType =>
      // Canonicalize values first so a non-orderable value type (e.g. variant) is orderable before
      // array_sort; sorting the entries makes map comparison key-order-insensitive.
      array_sort(map_entries(transform_values(c, (_, v) => canonicalizeCol(v, mt.valueType))))
    case at: ArrayType => transform(c, x => canonicalizeCol(x, at.elementType))
    case st: StructType =>
      struct(st.fields.map(f => canonicalizeCol(c.getField(f.name), f.dataType).as(f.name)): _*)
    // Casting to string renders variant as JSON with object fields in alphabetical order, so
    // logically equal variants compare equal even when their stored bytes differ by key order.
    case dt if dt.typeName == "variant" =>
      c.cast(StringType)
    case _ => c
  }
}
