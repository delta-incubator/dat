# Authoring Guide

How to write new workload suites for the Delta workload generator.

## Table of Contents

- [Quick Start](#quick-start)
- [Suite Structure](#suite-structure)
- [Creating Tables](#creating-tables)
- [Generated Output Structure](#generated-output-structure)
- [Declaring Specs — Operation → JSON Correspondence](#declaring-specs--operation--json-correspondence)
- [Table Mutations](#table-mutations)
- [Tags and Naming](#tags-and-naming)
- [Patterns and Recipes](#patterns-and-recipes)
- [Validation](#validation)
- [Running Your Suite](#running-your-suite)
- [Debugging](#debugging)

---

## Quick Start

Create a suite in `src/test/scala/io/delta/workload/tables/`, extend `WorkloadTestSuite`, and run it:

```scala
// src/test/scala/io/delta/workload/tables/MyFeatureSuite.scala
package io.delta.workload.tables

import io.delta.workload.WorkloadTestSuite

class MyFeatureSuite extends WorkloadTestSuite("my_feature") {

  test("mf_basic") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'alice'), (2, 'bob')")

    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

}
```

```bash
WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly *MyFeatureSuite"
```

That's it. The framework handles table copying, spec capture, validation, expected data generation, and cleanup.

---

## Suite Structure

### WorkloadTestSuite

Each suite is a ScalaTest class extending `WorkloadTestSuite`:

```scala
package io.delta.workload.tables

import io.delta.workload.WorkloadTestSuite

class MySuite extends WorkloadTestSuite("suite_name") {

  test("test_id") {
    // Setup tables with SQL
    // Register tables
    // Declare specs
  }

  test("another_test") {
    // ...
  }

}
```

**Rules:**
- `suite_name` should be short and descriptive (used in output directory names)
- `test_id` must be unique within a suite
- Each test is independent — tables are cleaned up between tests
- Test failures don't stop the suite — all tests run regardless

### Available DSL

Inside a test body, these methods are available directly (via `WorkloadOps` trait):

| Category | Methods | Handle |
|----------|---------|--------|
| Spark | `spark` — the active `SparkSession` | — |
| SQL | `sql(statement)` | — |
| Table handles | `registerTable(name)`, `registerTableFromPath(path)` | → `TableHandle` |
| Read specs | `readSpec(t, ...)`, `snapshotSpec(t, ...)` | `TableHandle` → `SpecRef` |
| Checkpointing | `forceCheckpoint(tableName)` — triggers a checkpoint via DeltaLog | — |
| Mutations | `mutateTable(t) { dir => ... }`, `modifyCommitActions(t, version) { ... }` | `TableHandle` |

---

## Creating Tables

### Standard SQL

The most common approach — use Spark SQL:

```scala
test("example") {
  sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  sql("INSERT INTO tbl VALUES (1, 'alice'), (2, 'bob')")
  sql("UPDATE tbl SET name = 'updated' WHERE id = 1")
  sql("DELETE FROM tbl WHERE id = 2")

  val t = registerTable("tbl")
  readSpec(t)
}
```

The `sql()` method tracks CREATE TABLE statements for automatic cleanup.

### Multiple Tables in One Test

A test can register multiple tables. Each produces its own output directory:

```scala
test("join_scenario") {
  sql("CREATE TABLE source (id INT, val STRING) USING delta")
  sql("INSERT INTO source VALUES (1, 'new')")
  sql("CREATE TABLE target (id INT, val STRING) USING delta")
  sql("INSERT INTO target VALUES (1, 'old')")
  sql("""MERGE INTO target t USING source s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET val = s.val""")

  val src = registerTable("source")
  val tgt = registerTable("target")
  readSpec(src)
  readSpec(tgt)
  snapshotSpec(tgt)
}
```

Output: `join_scenario_source/` and `join_scenario_target/`.

If a test registers only one table, the output directory is just the test name (no table suffix).

### DeltaLog API

For operations not exposed via SQL (checkpoints, direct log access), use `spark` to access the active SparkSession:

```scala
import org.apache.spark.sql.delta.DeltaLog

test("checkpoint_test") {
  sql("CREATE TABLE tbl (id INT) USING delta")
  sql("INSERT INTO tbl VALUES (1), (2), (3)")

  val loc = spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  DeltaLog.forTable(spark, loc).checkpoint()
  DeltaLog.clearCache()

  val t = registerTable("tbl")
  readSpec(t)
  checkpoint(t, version = 1)
}
```

Or use the convenience DSL method:

```scala
test("checkpoint_test") {
  sql("CREATE TABLE tbl (id INT) USING delta")
  sql("INSERT INTO tbl VALUES (1), (2), (3)")

  forceCheckpoint("tbl")

  val t = registerTable("tbl")
  readSpec(t)
}
```

**Important:** Always call `DeltaLog.clearCache()` after direct DeltaLog operations.

---

## Generated Output Structure

Each test produces one directory per registered table:

```
<test_name>/
├── table_info.json              # Discovery metadata (protocol, schema, tags, logInfo)
├── delta/                       # The Delta table
│   └── _delta_log/
│       ├── 00000000000000000000.json
│       ├── 00000000000000000001.json
│       └── ...
├── specs/                       # One JSON file per declared spec
│   ├── <test>_read.json
│   ├── <test>_read_v0.json
│   ├── <test>_snapshot.json
│   └── ...
├── expected/                    # Golden data per spec (keyed by spec name)
│   ├── <test>_read/
│   │   ├── expected_data/       # Parquet files — the rows the read should return
│   │   └── expected_metadata/   # Parquet file — AddFile actions that were scanned
│   ├── <test>_read_v0/
│   │   └── expected_data/
│   └── <test>_checkpoint_v2/
│       ├── expected_checkpoint/ # Raw checkpoint files copied from _delta_log
│       ├── expected_data/       # Parquet files — the rows at that version
│       └── expected_metadata/   # Parquet file — AddFile actions at that version
└── repro/
    └── generate.scala           # Source script that produced this table
```

If a test registers **one** table, the directory is just `<test_name>/`. If it registers **multiple**, each gets `<test_name>_<table_name>/`.

`table_info.json` is the entry point for harness discovery. It contains the protocol version, schema, table properties, log statistics, and tags. Harnesses that can't handle a given `minReaderVersion` or `readerFeatures` should skip the workload.

---

## Declaring Specs — Operation → JSON Correspondence

Every DSL method maps to a spec JSON file in `specs/` and (for success cases) an `expected/` directory. All parameters below are optional unless noted.

### `readSpec(t, ...)`

```scala
readSpec(t,
  version: Long,          // time travel by version
  timestamp: String,      // time travel by timestamp (mutually exclusive with version)
  predicate: String,      // SQL WHERE clause
  columns: Seq[String],   // column projection
  name: String            // override auto-generated spec name
)
```

Produces `specs/<test>_<name>.json`:

```json
{
  "type": "read",
  "version": 2,
  "predicate": "id > 5",
  "columns": ["id", "name"],
  "expected": { "rowCount": 42, "fileCount": 3, "filesSkipped": 7 }
}
```

Or on error (corrupt table, bad version, unsupported protocol):

```json
{
  "type": "read",
  "version": 999,
  "expectedError": { "errorCode": "DELTA_VERSION_NOT_FOUND", "errorMessage": "..." }
}
```

Auto-naming: `read` → `read_v0` → `read_id_gt_5` → `read_cols_id` → `read_v2_id_gt_5_cols_id_name`.

Expected data: `expected/<test>_<name>/expected_data/*.parquet` (multiset comparison, order-independent) and `expected/<test>_<name>/expected_metadata/*.parquet` (scanned AddFile actions).

### `snapshotSpec(t, ...)`

```scala
snapshotSpec(t,
  version: Long,          // snapshot at this version
  timestamp: String       // snapshot at this timestamp
)
```

Produces `specs/<test>_snapshot[_v<N>].json`:

```json
{
  "type": "snapshot",
  "version": 3,
  "expected": {
    "protocol": { "minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": [...], "writerFeatures": [...] },
    "metadata": { "id": "...", "format": {...}, "schemaString": "...", "partitionColumns": [...], "configuration": {...}, "createdTime": ... }
  }
}
```

If neither `version` nor `timestamp` is given, captures the latest snapshot. If no `snapshot()` call is made at all, a default latest-version snapshot is generated automatically.

---

## Table Mutations

Mutations modify the copied table *after* it's copied from the Spark warehouse but *before* specs are captured. They run against the **output copy**, not the live Spark table — this is intentional because mutations are often destructive (deleting parquet files, corrupting commit JSON, injecting invalid actions) and would break Spark if applied to the source table. The pipeline is: SQL builds the table → copy to output → mutate the copy → capture specs from the mutated state.

### mutateTable — File-Level Manipulation

```scala
test("missing_file", "Test missing data file") {
  sql("CREATE TABLE tbl (id INT) USING delta")
  sql("INSERT INTO tbl VALUES (1), (2)")

  val t = registerTable("tbl")

  mutateTable(t) { tableDir =>
    // tableDir is the path to the copied delta/ directory
    val files = java.nio.file.Files.list(tableDir)
    files.iterator().asScala
      .find(_.toString.endsWith(".parquet"))
      .foreach(java.nio.file.Files.delete)
    files.close()
  }

  readSpec(t)  // Will capture an error spec since a file is missing
}
```

### modifyCommitActions — Action-Level Manipulation

The modifier receives the full list of `(actionType, innerNode)` pairs for a commit and returns the (possibly reordered, filtered, or modified) list to write back.

```scala
// Modify add actions
modifyCommitActions(t, version = 1) { actions =>
  actions.map { case ("add", node) =>
    node.put("stats", """{"numRecords":999}"""); ("add", node)
    case other => other
  }
}

// Drop all add actions
modifyCommitActions(t, version = 1) { _.filter(_._1 != "add") }

// Reorder — put metaData last
modifyCommitActions(t, version = 0) { actions =>
  val (meta, rest) = actions.partition(_._1 == "metaData")
  rest ++ meta
}
```

### Direct JSON Manipulation

For complete control, manipulate commit files directly inside `mutateTable`:

```scala
test("inject_txn", "Inject txn action") {
  sql("CREATE TABLE tbl (id INT) USING delta")
  sql("INSERT INTO tbl VALUES (1)")

  val t = registerTable("tbl")

  mutateTable(t) { tableDir =>
    val logDir = tableDir.resolve("_delta_log")
    val commitFile = logDir.resolve("00000000000000000001.json")
    val content = new String(java.nio.file.Files.readAllBytes(commitFile), "UTF-8")
    val txnLine = """{"txn":{"appId":"test-app","version":42}}"""
    java.nio.file.Files.write(commitFile,
      (content.trim + "\n" + txnLine + "\n").getBytes("UTF-8"))
  }

}
```

CRC sidecar files are automatically deleted after all mutations run — no manual cleanup needed.

---

## Tags and Naming

### Tags

Tags can be added via ScalaTest's tagging mechanism or by including them in `table_info.json` via the `tags` parameter on `registerTable()`:

```scala
test("dv_delete") {
  // ...
  val t = registerTable("tbl", tags = Seq("dv", "delete"))
}
```

Common tags used across the codebase:

| Tag | Meaning |
|-----|---------|
| `dv` | Deletion vectors |
| `column_mapping` | Column mapping (name or id mode) |
| `checkpoint` | Checkpoint scenarios |
| `merge` | MERGE operations |
| `partitioned` | Partitioned tables |
| `schema_evolution` | Schema changes |
| `error` | Expected error scenarios |
| `logReplay` | Log replay scenarios |
| `corruption` | Corruption/resilience testing |

### Naming Conventions

- **Suite names:** lowercase, underscored (`reads`, `deletion_vectors`, `write_basic`)
- **Test IDs:** short, prefixed by suite abbreviation (`dv_001`, `lr_basic`, `cdc_merge`)
- **Spec names:** auto-generated from parameters (see [Spec Reference](spec-reference.md#naming-conventions))

---

## Patterns and Recipes

### Testing Error Conditions

Just read from a corrupted/invalid table — errors are captured automatically:

```scala
test("err_missing_version") {
  sql("CREATE TABLE tbl (id INT) USING delta")
  sql("INSERT INTO tbl VALUES (1)")
  sql("INSERT INTO tbl VALUES (2)")

  val t = registerTable("tbl")

  mutateTable(t) { tableDir =>
    val commit1 = tableDir.resolve("_delta_log/00000000000000000001.json")
    java.nio.file.Files.delete(commit1)
  }

  readSpec(t).assertError()  // Fails generation if the spec is not an error
}
```

### Asserting on Captured Results

All spec methods return a typed `SpecRef[T]` (e.g. `SpecRef[ReadSpec]`, `SpecRef[SnapshotSpec]`). You can attach assertions that run after capture using the actual spec case classes:

```scala
// Assert the spec captured an error
readSpec(t).assertError()

// Assert on the typed ReadSpec
readSpec(t, predicate = "id > 5").assert { spec: ReadSpec =>
  require(spec.expected.isDefined, "Expected success, not error")
  require(spec.expected.get.rowCount > 0, s"Expected rows")
}

// Assert on snapshot protocol/metadata
snapshotSpec(t).assert { spec: SnapshotSpec =>
  require(spec.expected.isDefined)
}

// Ignoring the return is fine — existing call sites are unaffected
readSpec(t)
snapshotSpec(t)
```

Assertions are checked during generation after the spec is written. Failed assertions cause the test to fail.

### Testing Data Skipping

Insert data across multiple files and use predicates that should skip some:

```scala
test("skipping_basic") {
  sql("CREATE TABLE tbl (id INT) USING delta")
  sql("INSERT INTO tbl VALUES (1), (2), (3)")    // File 1: min=1, max=3
  sql("INSERT INTO tbl VALUES (10), (20), (30)")  // File 2: min=10, max=30

  val t = registerTable("tbl")
  readSpec(t, predicate = "id > 5")  // Should skip file 1
  readSpec(t, predicate = "id < 5")  // Should skip file 2
  readSpec(t)                        // Should read both files
}
```

### Testing Schema Evolution

Create a table, evolve the schema, then read at different versions:

```scala
test("schema_add_col") {
  sql("CREATE TABLE tbl (id INT) USING delta")
  sql("INSERT INTO tbl VALUES (1), (2)")
  sql("ALTER TABLE tbl ADD COLUMN name STRING")
  sql("INSERT INTO tbl VALUES (3, 'charlie')")

  val t = registerTable("tbl")
  readSpec(t)                // 3 rows, name is null for old rows
  readSpec(t, version = 1)   // 2 rows, no name column
  for (v <- 0L to 3) snapshotSpec(t, version = v)
}
```

### Testing Checkpoint Scenarios

```scala
test("checkpoint_basic") {
  sql("CREATE TABLE tbl (id INT) USING delta")
  sql("INSERT INTO tbl VALUES (1)")
  sql("INSERT INTO tbl VALUES (2)")
  sql("INSERT INTO tbl VALUES (3)")

  forceCheckpoint("tbl")  // Explicit checkpoint trigger

  val t = registerTable("tbl")
  readSpec(t)
  snapshotSpec(t)
}
```

### Testing Deletion Vectors

```scala
test("dv_basic") {
  sql("""CREATE TABLE tbl (id INT, val STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
  sql("DELETE FROM tbl WHERE id = 2")  // Creates a DV

  val t = registerTable("tbl")
  readSpec(t)                     // Should return rows 1, 3
  readSpec(t, version = 1)        // Should return rows 1, 2, 3
  readSpec(t, predicate = "id > 1")  // Should return row 3 only
  snapshotSpec(t)
}
```

### Testing Column Mapping

```scala
test("cm_rename") {
  sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
    TBLPROPERTIES (
      'delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2',
      'delta.minWriterVersion' = '5'
    )""")
  sql("INSERT INTO tbl VALUES (1, 'alice')")
  sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
  sql("INSERT INTO tbl VALUES (2, 'bob')")

  val t = registerTable("tbl")
  readSpec(t)
  readSpec(t, version = 1)
  for (v <- 0L to 3) snapshotSpec(t, version = v)
}
```

### Programmatic / Combinatorial Generation

Since `test(...)` is just a method call, you can generate workloads programmatically with loops. This is the recommended way to get exhaustive coverage across feature combinations.

**Single dimension — DV on/off:**

```scala
for (dvEnabled <- Seq(true, false)) {
  val suffix = if (dvEnabled) "dv" else "no_dv"
  val props = if (dvEnabled) "'delta.enableDeletionVectors' = 'true'" else ""

  test(s"delete_$suffix") {
    sql(s"CREATE TABLE tbl (id INT, val STRING) USING delta TBLPROPERTIES ($props)")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 2")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "id > 1")
    snapshotSpec(t)
  }
}
```

**Cross-product — partition × DV × predicate type:**

```scala
val partitionModes = Seq(
  ("unpartitioned", Seq.empty[String]),
  ("partitioned",   Seq("region")))
val dvModes = Seq(true, false)
val predicates = Seq(
  ("eq",    "id = 1"),
  ("range", "id > 5"),
  ("null",  "val IS NULL"))

for {
  (partLabel, partCols) <- partitionModes
  dvEnabled             <- dvModes
  (predLabel, predExpr) <- predicates
} {
  val name = s"read_${partLabel}_dv${dvEnabled}_$predLabel"
  val props = Map(
    "delta.enableDeletionVectors" -> dvEnabled.toString
  ).map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")

  test(name) {
    val partClause = if (partCols.nonEmpty)
      s"PARTITIONED BY (${partCols.mkString(",")})" else ""
    sql(s"""CREATE TABLE tbl (id INT, val STRING, region STRING) USING delta
      $partClause TBLPROPERTIES ($props)""")
    sql("INSERT INTO tbl VALUES (1,'a','us'),(2,'b','eu'),(3,NULL,'us')")
    sql("INSERT INTO tbl VALUES (4,'d','eu'),(5,'e','us'),(6,NULL,'eu')")
    sql("DELETE FROM tbl WHERE id = 3")

    val t = registerTable("tbl")
    readSpec(t, predicate = predExpr)
    readSpec(t)
    snapshotSpec(t)
  }
}
```

This generates 12 independent workloads (`2 partition modes × 2 DV modes × 3 predicates`), each with its own output directory, spec files, and expected data.

---

## Validation

Every spec is self-validated during generation:

1. **Read specs:** The framework reads the table, writes expected Parquet, re-reads, and compares multisets
2. **Snapshot specs:** The framework loads the snapshot, writes the spec, re-loads, and deep-compares protocol/metadata JSON
3. **Error specs:** The framework triggers the error, writes the spec, re-triggers, and verifies the same error occurs

If validation fails, the test output is deleted (so it auto-retries on next run) and the failure is reported.

### Validation Warnings

Some conditions produce warnings rather than failures:
- Error code mismatch on re-validation (operation failed both times, but with different Spark error codes)

---

## Running Your Suite

### Quick Run (one suite)

```bash
WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly *MyFeatureSuite"
```

### Run a specific test

```bash
WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly *MyFeatureSuite -- -t mf_basic"
```

### Force Regeneration

```bash
WORKLOAD_FORCE=true WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly *MyFeatureSuite"
```

Without `WORKLOAD_FORCE=true`, tests that already have `table_info.json` in the output are skipped.

### Run All Suites

```bash
WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly io.delta.workload.tables.*"
```

### Running Framework Tests

```bash
cd workload-generator
sbt test
```

This runs `WorkloadGeneratorSuite` which tests the framework itself.

---

## Debugging

### Test Output Not Generated

1. Check the console output for `[FAIL]` lines
2. Failed tests clean up their output — re-run to see the error again
3. Set `WORKLOAD_FORCE=true` to regenerate everything

### Validation Failures

The framework prints detailed diff information on multiset mismatches:

```
Validation FAILED for my_test_read: row-level mismatch (expected 3, got 2)
  Missing rows: 1
    {"id":3,"name":"charlie"}
```

### Spark Errors During Setup

If your SQL fails during table creation:
1. Test the SQL in `sbt console` first (then `import io.delta.workload._`)
2. Check that table names don't collide (each test cleans up, but verify)
3. Ensure required table properties are set for the features you're using

### DeltaLog Cache Issues

If you use `DeltaLog.forTable()` directly, always call `DeltaLog.clearCache()` before and after. Stale cached state is the most common source of confusing failures.

### Debugging `WorkloadContext.current`

If you get "No active WorkloadContext", you're calling a DSL method outside a test body. Ensure all `sql()`, `registerTable()`, `readSpec()`, etc. calls are inside `test(...) { ... }` blocks.
