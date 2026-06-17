# Delta Workload Generator

Generate acceptance test workloads for Delta implementations (e.g., [delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs)) from Apache Spark.

Write a script that creates Delta tables with normal SQL, declare what specs to capture, and the framework generates complete workload directories with expected data validated row-for-row against Spark.

## Documentation

| Document | Description |
|----------|-------------|
| **[Spec Format Reference](docs/spec-reference.md)** | JSON schema for the `read`, `snapshot`, and `write` spec types, with examples |
| **[Harness Implementation Guide](docs/harness-implementation-guide.md)** | Step-by-step guide to build a test harness that runs workloads against your engine, with a Rust example |
| **[Authoring Guide](docs/authoring-guide.md)** | How to write new workload suites, patterns, recipes, and debugging tips |

## Requirements

- Java 17+
- Apache Spark 4.1.x with Scala 2.13
- Delta Spark 4.1.0 (pulled automatically via `--packages`)
- sbt 1.9+ (for building)

## Quick Start

```bash
cd workload-generator

# Run one suite
WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly *ReadsSuite"

# Run all table suites
WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly io.delta.workload.tables.*"

# Run a specific test
WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly *ReadsSuite -- -t read_basic"

# Force regeneration (even if output exists)
WORKLOAD_FORCE=true WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly *ReadsSuite"
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      WorkloadTestSuite                               │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │ test("name") {                                                  ││
│  │   sql("CREATE TABLE ...")      // Setup tables via SQL          ││
│  │   val t = registerTable("tbl") // Get table handle              ││
│  │   readSpec(t)                  // Declare read spec             ││
│  │   snapshotSpec(t)              // Declare snapshot spec         ││
│  │ }                                                               ││
│  └─────────────────────────────────────────────────────────────────┘│
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     WorkloadGenerator                                │
│  1. Copy Delta table to output directory                            │
│  2. For each declared spec:                                         │
│     - Execute against copied table                                  │
│     - Capture results/expected data                                 │
│     - Validate by re-execution                                      │
│     - Write spec JSON + expected data                               │
│  3. Write table_info.json                                           │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Output Directory                                  │
│  <testname>/                                                        │
│    delta/           # Copied Delta table                            │
│    specs/           # Spec JSON files                               │
│    expected/        # Parquet expected data                         │
│    table_info.json  # Table metadata                                │
│    repro/           # Repro placeholder                             │
└─────────────────────────────────────────────────────────────────────┘
```

### Core Components

| Component | Purpose |
|-----------|---------|
| `WorkloadTestSuite` | ScalaTest base class with workload generation integration |
| `WorkloadOps` | DSL trait: `sql()`, `registerTable()`, `readSpec()`, `snapshotSpec()` |
| `WorkloadGenerator` | Orchestrates table copy, spec capture, and validation |
| `ReadCapture` | Captures read specs with expected row data |
| `SnapshotCapture` | Captures snapshot specs with protocol/metadata |
| `TableInfoWriter` | Writes table metadata (schema, protocol, stats) |
| `JsonUtil` | Shared JSON utilities, multiset comparison |

## Writing a Suite

Each suite is a ScalaTest class extending `WorkloadTestSuite`. Each `test` creates tables, declares specs, and the framework handles the rest.

```scala
// src/test/scala/io/delta/workload/tables/ReadsSuite.scala
package io.delta.workload.tables

import io.delta.workload.WorkloadTestSuite

class ReadsSuite extends WorkloadTestSuite("reads") {

  test("read_basic") {
    sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    sql("INSERT INTO tbl VALUES (3, 'c')")

    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    readSpec(t, predicate = "id > 1")
    snapshotSpec(t)
  }

}
```

See the [Authoring Guide](docs/authoring-guide.md) for the complete DSL reference, patterns, and debugging tips.

### Test Semantics

- **Pass**: output kept, skipped on re-run (incremental)
- **Fail**: output deleted, auto-retries next run (no `--force` needed)
- **Each test is independent**: failures don't stop the suite

## Output Structure

Each `registerTable()` call produces a flat top-level directory:

```
read_basic/
  delta/                                  # Copy of the Delta table
    _delta_log/
      00000000000000000000.json
      ...
    part-00000-*.parquet
    ...
  specs/
    read_basic_read.json                  # Read spec (latest)
    read_basic_read_v1.json               # Read at version 1
    read_basic_read_id_gt_1.json          # Filtered read
    read_basic_snapshot.json              # Snapshot construction
  expected/
    read_basic_read/
      expected_data/                      # Parquet expected rows
      expected_metadata/                  # AddFile actions
    ...
  table_info.json                         # Table schema, protocol, stats
  repro/generate.scala                    # Placeholder (not a runnable script yet)
```

See the [Spec Format Reference](docs/spec-reference.md) for the complete JSON schema of every spec type.

## Spec Types

| Type | What It Tests | Details |
|------|--------------|---------|
| **Read** | Data reads with time travel, predicates, column projection, data skipping | [Reference](docs/spec-reference.md#read-spec) |
| **Snapshot** | Protocol and metadata reconstruction from log replay | [Reference](docs/spec-reference.md#snapshot-spec) |
| **Write** | Table construction via a portable sequence of commits (create/replace/insert/delete/update/schema-evolution/properties and low-level `commit`s), replayed into a fresh table | [Reference](docs/spec-reference.md#write-spec) |

The write DSL (`createTableOp`, `insertOp`, `commitOp`, …) is exposed by `WorkloadOps`; see the [Authoring Guide](docs/authoring-guide.md) for how to author write workloads. A `read`/`snapshot` spec captured against a write-built table carries a `writeSpec` pointer and is validated *portably* (replay the sibling write spec into a fresh table, then compare).

## Workload Suites

Workload suites are in `src/test/scala/io/delta/workload/tables/`. Each suite extends `WorkloadTestSuite` and covers a specific Delta feature area (reads, deletion vectors, column mapping, time travel, checkpoints, data skipping, protocol versions, merge, schema evolution, type widening, variant, row tracking, in-commit timestamps, and more).

## Building a Test Harness

If you're implementing a Delta engine and want to use these workloads for acceptance testing, see the [Harness Implementation Guide](docs/harness-implementation-guide.md). It walks through:

1. Discovering and filtering workloads
2. Implementing each spec type handler
3. Multiset row comparison
4. Error-spec handling (assert that an error occurs, not a specific code)
5. CI integration
6. Incremental adoption strategy

## CI Integration

```bash
WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly io.delta.workload.tables.*"
```

## Troubleshooting

### Spark version mismatch
If you see `NoSuchMethodError` related to Delta APIs, ensure:
- build.sbt uses Delta 4.1.0 / Spark 4.1.x
- Your Spark installation matches (Spark 4.1.x with Scala 2.13)

### Memory issues
For large workloads:
```bash
export SBT_OPTS="-Xmx4g"
WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly *LargeSuite"
```

### Re-running failed tests
Failed tests auto-cleanup their output. Just re-run:
```bash
WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly *ReadsSuite"
```

Use `WORKLOAD_FORCE=true` to regenerate all tests (including passed ones):
```bash
WORKLOAD_FORCE=true WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly *ReadsSuite"
```
