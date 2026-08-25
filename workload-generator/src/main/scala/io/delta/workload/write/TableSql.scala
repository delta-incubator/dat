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

package io.delta.workload.write

import org.apache.spark.sql.types.StructType

/**
 * Renders a typed table definition into the Delta `CREATE TABLE` SQL used by both capture
 * ([[WorkloadOps]]) and replay ([[WorkloadValidator]]). The one home for "typed -> SQL DDL".
 */
private[workload] object TableSql {

  /** `CREATE [OR REPLACE] TABLE ref (cols) USING delta [PARTITIONED BY …] [TBLPROPERTIES …]`. */
  def createTable(
      ref: String, schema: StructType, partitionColumns: Seq[String],
      properties: Map[String, String], orReplace: Boolean = false): String =
    s"CREATE ${if (orReplace) "OR REPLACE " else ""}TABLE $ref (${schema.toDDL}) USING delta" +
      partitionedBy(partitionColumns) + tblProperties(properties)

  /** `CREATE OR REPLACE TABLE ref USING delta [PARTITIONED BY …] [TBLPROPERTIES …] AS <select>` (RTAS). */
  def replaceTableAsSelect(
      ref: String, select: String, partitionColumns: Seq[String],
      properties: Map[String, String]): String =
    s"CREATE OR REPLACE TABLE $ref USING delta" +
      partitionedBy(partitionColumns) + tblProperties(properties) + s" AS $select"

  private def partitionedBy(columns: Seq[String]): String =
    if (columns.nonEmpty) s" PARTITIONED BY (${columns.mkString(", ")})" else ""

  private def tblProperties(properties: Map[String, String]): String =
    if (properties.nonEmpty) {
      s" TBLPROPERTIES (${properties.map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")})"
    } else ""
}
