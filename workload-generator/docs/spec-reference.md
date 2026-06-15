# Spec Format Reference

This document defines every spec type produced by the workload generator. Each spec is a self-contained JSON file describing an operation to execute against a Delta table and the expected result. Specs live in the `specs/` directory of each workload output.

All specs follow the same top-level pattern:

```json
{
  "type": "<spec_type>",
  ...operation parameters...,
  "expected": { ...success expectations... },
  "expectedError": { "errorCode": "...", "errorMessage": "..." }
}
```

Exactly one of `expected` or `expectedError` is present. The other is omitted (not `null`).

---

## Table of Contents

- [Common Types](#common-types)
- [Read Spec](#read-spec)
- [Snapshot Spec](#snapshot-spec)
- [Write Spec](#write-spec)
- [table_info.json](#table_infojson)
- [Expected Data Layout](#expected-data-layout)

---

## Common Types

### SpecError

Every error spec uses this shape:

| Field | Type | Description |
|-------|------|-------------|
| `errorCode` | `string` | Error class identifier. Spark implementations use `SparkThrowable.getErrorClass`. Non-Spark engines should map to equivalent codes. |
| `errorMessage` | `string` | Human-readable message. Engines need not match this exactly — it is informational. |

```json
{
  "errorCode": "DELTA_TABLE_NOT_FOUND",
  "errorMessage": "Delta table not found at path /tmp/missing"
}
```

The `errorCode` is the primary field for harness assertion. Error messages vary across implementations and should be treated as advisory.

### ProtocolInfo

Appears in snapshot and table_info specs:

| Field | Type | Description |
|-------|------|-------------|
| `minReaderVersion` | `int` | Minimum reader protocol version (1–3) |
| `minWriterVersion` | `int` | Minimum writer protocol version (1–7) |
| `readerFeatures` | `string[]?` | Reader features (present only when minReaderVersion ≥ 3). Sorted alphabetically. |
| `writerFeatures` | `string[]?` | Writer features (present only when minWriterVersion ≥ 7). Sorted alphabetically. |

```json
{
  "minReaderVersion": 3,
  "minWriterVersion": 7,
  "readerFeatures": ["deletionVectors"],
  "writerFeatures": ["deletionVectors"]
}
```

---

## Read Spec

**Type:** `"read"`

Tests reading data from a Delta table with optional time travel, predicate pushdown, and column projection.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | yes | Always `"read"` |
| `version` | `long` | no | Time travel to this version |
| `timestamp` | `string` | no | Time travel to this timestamp (format: `yyyy-MM-dd HH:mm:ss.SSS`) |
| `predicate` | `string` | no | SQL WHERE clause to apply (e.g., `"id > 5"`) |
| `columns` | `string[]` | no | Columns to select (projection pushdown) |
| `expected` | `ReadExpected` | no | Present on success |
| `expectedError` | `SpecError` | no | Present on expected failure |

Only one of `version` or `timestamp` may be set.

### ReadExpected

| Field | Type | Description |
|-------|------|-------------|
| `rowCount` | `long` | Total rows returned |
| `fileCount` | `int` | Number of data files actually scanned |
| `filesSkipped` | `long` | Files skipped via data skipping (stats-based pruning) |

The relationship `fileCount + filesSkipped = total files in table at that version` always holds for unpartitioned tables. For partitioned tables, `filesSkipped` reflects both partition pruning and stats-based skipping.

### Expected Data

When `expected` is present, the directory `expected/<spec_name>/` contains:

- **`expected_data/`** — Parquet files with the exact rows the read should return. Row order is irrelevant; comparison is multiset-based (each row appears the correct number of times). Capped at 5,000,000 rows.
- **`expected_metadata/`** — Parquet file with one column `action` containing the JSON `AddFile` actions for files that were scanned (not skipped). Use this to validate data skipping behavior.

### Examples

**Latest version, no filters:**

```json
{
  "type": "read",
  "expected": {
    "rowCount": 3,
    "fileCount": 1,
    "filesSkipped": 0
  }
}
```

**Time travel by version:**

```json
{
  "type": "read",
  "version": 0,
  "expected": {
    "rowCount": 100,
    "fileCount": 2,
    "filesSkipped": 0
  }
}
```

**Time travel by timestamp:**

```json
{
  "type": "read",
  "timestamp": "2025-01-15 10:30:00.000",
  "expected": {
    "rowCount": 50,
    "fileCount": 1,
    "filesSkipped": 0
  }
}
```

**Predicate pushdown with data skipping:**

```json
{
  "type": "read",
  "predicate": "id > 500",
  "expected": {
    "rowCount": 500,
    "fileCount": 5,
    "filesSkipped": 5
  }
}
```

**Column projection:**

```json
{
  "type": "read",
  "columns": ["id", "name"],
  "expected": {
    "rowCount": 1000,
    "fileCount": 10,
    "filesSkipped": 0
  }
}
```

**Combined: version + predicate + columns:**

```json
{
  "type": "read",
  "version": 2,
  "predicate": "category = 'electronics'",
  "columns": ["id", "category", "price"],
  "expected": {
    "rowCount": 42,
    "fileCount": 3,
    "filesSkipped": 7
  }
}
```

**Error: version does not exist:**

```json
{
  "type": "read",
  "version": 999,
  "expectedError": {
    "errorCode": "DELTA_VERSION_NOT_FOUND",
    "errorMessage": "Cannot find version 999"
  }
}
```

**Error: unsupported protocol:**

```json
{
  "type": "read",
  "expectedError": {
    "errorCode": "DELTA_UNSUPPORTED_FEATURES_FOR_READ",
    "errorMessage": "Required reader features not supported: [futureFeature]"
  }
}
```

**Error: corrupt data file:**

```json
{
  "type": "read",
  "expectedError": {
    "errorCode": "FAILED_READ_FILE",
    "errorMessage": "Failed to read file: part-00000-abc.parquet"
  }
}
```

---

## Snapshot Spec

**Type:** `"snapshot"`

Tests constructing a table snapshot at a given version, validating the protocol and metadata are correctly reconstructed from the log.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | yes | Always `"snapshot"` |
| `version` | `long` | no | Snapshot at this version (latest if omitted and no timestamp) |
| `timestamp` | `string` | no | Snapshot at this timestamp |
| `expected` | `SnapshotExpected` | no | Present on success |
| `expectedError` | `SpecError` | no | Present on expected failure |

### SnapshotExpected

| Field | Type | Description |
|-------|------|-------------|
| `protocol` | `object` | The protocol action from the Delta log. Contains `minReaderVersion`, `minWriterVersion`, and optionally `readerFeatures`/`writerFeatures`. |
| `metadata` | `object` | The metaData action from the Delta log. Contains `id`, `name`, `format`, `schemaString`, `partitionColumns`, `configuration`, `createdTime`. |

These are the raw Delta protocol and metadata JSON structures — not simplified. Your engine should produce equivalent JSON.

### Examples

**Latest snapshot:**

```json
{
  "type": "snapshot",
  "version": 3,
  "expected": {
    "protocol": {
      "minReaderVersion": 1,
      "minWriterVersion": 2
    },
    "metadata": {
      "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "format": { "provider": "parquet", "options": {} },
      "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
      "partitionColumns": [],
      "configuration": {},
      "createdTime": 1705000000000
    }
  }
}
```

**Snapshot with table features (v3/v7 protocol):**

```json
{
  "type": "snapshot",
  "version": 5,
  "expected": {
    "protocol": {
      "minReaderVersion": 3,
      "minWriterVersion": 7,
      "readerFeatures": ["deletionVectors"],
      "writerFeatures": ["deletionVectors"]
    },
    "metadata": {
      "id": "abc123",
      "format": { "provider": "parquet", "options": {} },
      "schemaString": "{\"type\":\"struct\",\"fields\":[...]}",
      "partitionColumns": ["date"],
      "configuration": {
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true"
      },
      "createdTime": 1705000000000
    }
  }
}
```

**Snapshot at version 0 (initial commit):**

```json
{
  "type": "snapshot",
  "version": 0,
  "expected": {
    "protocol": {
      "minReaderVersion": 1,
      "minWriterVersion": 2
    },
    "metadata": {
      "id": "...",
      "format": { "provider": "parquet", "options": {} },
      "schemaString": "...",
      "partitionColumns": [],
      "configuration": {},
      "createdTime": 1705000000000
    }
  }
}
```

**Error: unsupported reader feature:**

```json
{
  "type": "snapshot",
  "expectedError": {
    "errorCode": "DELTA_UNSUPPORTED_FEATURES_FOR_READ",
    "errorMessage": "Table requires reader feature 'unknownFeature' which is not supported"
  }
}
```

---

## Write Spec

**Type:** `"write"`

Tests Delta writer implementations by providing a sequence of write operations to replay. Unlike read specs (which verify reading an existing table), a write spec describes *how to construct* a table from scratch.

A write spec is a portable, implementation-agnostic recipe: it specifies what operations to perform (create table, insert, delete, update, etc.) declaratively, so any conforming Delta writer can interpret and execute it. After replaying all commits, the resulting table is compared against the expected data under `expected/latest/`.

The write spec is written to `write_spec.json` at the workload output root (not under `specs/`). The read and snapshot specs declared alongside a write workload are captured normally under `specs/`.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | yes | Always `"write"` |
| `commits` | `WriteCommit[]` | yes | Ordered list of commits to replay |

### WriteCommit

Each commit represents a single Delta transaction. There are two categories: high-level operations (SQL semantics) and the low-level `commit` (raw Delta actions).

#### High-Level Operations

These map to SQL-like operations. The writer translates them to appropriate Delta actions.

| Operation | Fields | Description |
|-----------|--------|-------------|
| `create_table` | `schema`, `partitionColumns?`, `properties?`, `dataFiles?` | Create a new table with the given schema |
| `insert` | `dataFiles?` | Append rows from data files |
| `update` | `predicate`, `set` | Update rows matching predicate |
| `delete` | `predicate` | Delete rows matching predicate |
| `evolve_schema` | `addColumns?`, `renameColumns?`, `dropColumns?` | Modify table schema |
| `update_properties` | `set?`, `remove?` | Modify table properties |
| `restore` | `version` | Restore table to a previous version |

The reference validator does not replay `restore`: an independent replay generates different file paths, so RESTORE-to-version would reference paths that do not exist in the replayed table. Conforming consumers may implement it.

#### Low-Level Operation

| Operation | Fields | Description |
|-----------|--------|-------------|
| `commit` | `schema?`, `tableProperties?`, `txn?`, `addFiles?`, `removeFiles?`, `addDomainMetadata?`, `removeDomainMetadata?` | Directly specify Delta actions. `addFiles[].dataFile` must reference Parquet already in the table (e.g. produced by a prior `insert`); capture copies it under `data/commit_N/`. `removeFiles[].path` must target a file added by a prior low-level `commit`; paths from `insert`/`create_table` are writer-generated and not stable across replay. Deletion vectors are not honored. |

### WriteCommit Fields

| Field | Type | Description |
|-------|------|-------------|
| `operation` | `string` | Operation type (see above) |
| `schema` | `object` | Table schema in Delta JSON format |
| `partitionColumns` | `string[]` | Partition column names |
| `properties` | `map` | Table properties (e.g., `delta.enableDeletionVectors`) |
| `dataFiles` | `string[]` | Relative paths to Parquet data files under `data/`. Files for a partitioned commit keep their `col=val/` directory prefix |
| `predicate` | `string` | SQL WHERE clause for update/delete |
| `set` | `map` | Column assignments for update (column → expression) or properties to set |
| `remove` | `string[]` | Property names to remove |
| `addColumns` | `object[]` | Columns to add (each with `name`, `type`, `nullable`) |
| `renameColumns` | `map` | Column renames (old name → new name) |
| `dropColumns` | `string[]` | Column names to drop |
| `version` | `long` | Target version for restore |
| `tableProperties` | `map` | Properties for low-level commit |
| `txn` | `AppTxn` | Application transaction for idempotent writes |
| `addFiles` | `AddFileAction[]` | Files to add (low-level) |
| `removeFiles` | `RemoveFileAction[]` | Files to remove (low-level) |
| `addDomainMetadata` | `AddDomainMetadata[]` | Domain metadata to add |
| `removeDomainMetadata` | `string[]` | Domain names to remove |

### AddFileAction (Low-Level)

| Field | Type | Description |
|-------|------|-------------|
| `dataFile` | `string` | Relative path to Parquet file under `data/` |
| `partitionValues` | `map?` | Partition values for this file |
| `dataChange` | `boolean?` | Whether this is a data change (default true) |

### RemoveFileAction (Low-Level)

| Field | Type | Description |
|-------|------|-------------|
| `path` | `string` | Path of file to remove |
| `dataChange` | `boolean?` | Whether this is a data change (default true) |

`path` must target a file added by a prior low-level `commit`; paths from `insert`/`create_table` are writer-generated and not stable across replay.

### Expected Data

The final table state after replaying all commits is captured under `expected/latest/`:

- `table_content/` — Parquet files with the expected rows
- `table_version_metadata.json` — the latest snapshot's `protocol` and `metadata`

### Comparison Semantics

The capture comes from one writer, but a write spec must validate against any conforming Delta writer, so a consumer compares the replayed snapshot against `expected` by capability rather than byte equality:

- **`protocol`** — feature-superset plus version-floor, not exact. The replay must satisfy `minReaderVersion >= expected` and `minWriterVersion >= expected`, and its `readerFeatures`/`writerFeatures` must be supersets of the expected sets (missing arrays count as empty; membership is order-insensitive). A higher version or extra features is allowed.
- **`configuration`** — only the keys the write spec's own commits declared are checked: `create_table.properties` plus `update_properties.set`, minus `update_properties.remove`. Each declared key must be present and equal; each removed key must be absent. Engine-injected default properties are ignored. If the spec declared no properties, nothing is checked.
- **rows** (`table_content/`) — multiset comparison; order is irrelevant, but each row must appear the correct number of times.

`schemaString`, `partitionColumns`, and `format` are compared exactly.

### Directory Structure

```
<test_name>/
├── write_spec.json              # The write spec
├── data/                        # Data files referenced by commits
│   ├── commit_0/
│   │   └── part-0000-xxx.parquet
│   ├── commit_1/
│   │   └── part-0000-yyy.parquet
│   └── ...
├── expected/
│   ├── latest/
│   │   ├── table_content/
│   │   └── table_version_metadata.json
│   └── <read_spec_name>/        # expected_data for each read spec
│       └── expected_data/
├── specs/                       # Read/snapshot specs for this workload
└── table_info.json
```

### Examples

**Create table and insert:**

```json
{
  "type": "write",
  "commits": [
    {
      "operation": "create_table",
      "schema": {
        "type": "struct",
        "fields": [
          { "name": "id", "type": "integer", "nullable": false, "metadata": {} },
          { "name": "name", "type": "string", "nullable": true, "metadata": {} }
        ]
      },
      "properties": { "delta.enableDeletionVectors": "true" }
    },
    {
      "operation": "insert",
      "dataFiles": ["data/commit_1/part-0000-abc.parquet"]
    }
  ]
}
```

**Delete with predicate:**

```json
{
  "type": "write",
  "commits": [
    { "operation": "create_table", "schema": { } },
    { "operation": "insert", "dataFiles": ["data/commit_1/part-0000-abc.parquet"] },
    { "operation": "delete", "predicate": "id > 100" }
  ]
}
```

**Update with SET:**

```json
{
  "type": "write",
  "commits": [
    { "operation": "create_table", "schema": { } },
    { "operation": "insert", "dataFiles": ["data/commit_1/part-0000-abc.parquet"] },
    {
      "operation": "update",
      "predicate": "status = 'pending'",
      "set": { "status": "'active'", "count": "count + 1" }
    }
  ]
}
```

**Schema evolution (add column):**

```json
{
  "type": "write",
  "commits": [
    {
      "operation": "create_table",
      "schema": {
        "type": "struct",
        "fields": [
          { "name": "id", "type": "integer", "nullable": false, "metadata": {} }
        ]
      }
    },
    { "operation": "insert", "dataFiles": ["data/commit_1/part-0000-abc.parquet"] },
    {
      "operation": "evolve_schema",
      "addColumns": [ { "name": "email", "type": "string", "nullable": true } ]
    },
    { "operation": "insert", "dataFiles": ["data/commit_3/part-0000-def.parquet"] }
  ]
}
```

**Partitioned table:**

```json
{
  "type": "write",
  "commits": [
    {
      "operation": "create_table",
      "schema": {
        "type": "struct",
        "fields": [
          { "name": "id", "type": "integer", "nullable": true, "metadata": {} },
          { "name": "region", "type": "string", "nullable": true, "metadata": {} },
          { "name": "revenue", "type": "integer", "nullable": true, "metadata": {} }
        ]
      },
      "partitionColumns": ["region"]
    },
    {
      "operation": "insert",
      "dataFiles": [
        "data/commit_1/region=east/part-0000-abc.parquet",
        "data/commit_1/region=west/part-0000-def.parquet"
      ]
    }
  ]
}
```

**Low-level commit with domain metadata:**

```json
{
  "type": "write",
  "commits": [
    {
      "operation": "create_table",
      "schema": { },
      "properties": { "delta.feature.domainMetadata": "supported" }
    },
    {
      "operation": "commit",
      "addFiles": [
        { "dataFile": "data/commit_1/part-0000-abc.parquet", "partitionValues": {}, "dataChange": true }
      ],
      "addDomainMetadata": [
        { "domain": "myApp.config", "configuration": "{\"version\": 1}" }
      ]
    }
  ]
}
```

**Low-level commit with application transaction:**

```json
{
  "type": "write",
  "commits": [
    { "operation": "create_table", "schema": { } },
    {
      "operation": "commit",
      "txn": { "appId": "streaming-job-1", "version": 42 },
      "addFiles": [ { "dataFile": "data/commit_1/part-0000-abc.parquet" } ]
    }
  ]
}
```

---

## table_info.json

Written to the workload output root. Provides metadata about the generated table for discovery and filtering.

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | `string` | Test identifier |
| `description` | `string` | Human-readable description |
| `schema` | `object` | Table schema (parsed JSON, not a string) |
| `protocol` | `ProtocolInfo` | Protocol version and features |
| `logInfo` | `LogInfo` | Log statistics |
| `properties` | `map` | Table properties (e.g., `delta.enableChangeDataFeed`) |
| `dataLayout` | `DataLayoutInfo` | Partition and clustering info |
| `tags` | `string[]?` | Feature tags for filtering |

### LogInfo

| Field | Type | Description |
|-------|------|-------------|
| `numAddFiles` | `long` | Active data files |
| `numRemoveFiles` | `long` | Tombstoned files |
| `sizeInBytes` | `long` | Total active data size |
| `numCommits` | `int` | Number of JSON commit files |
| `numActions` | `long` | Total actions across all commits |
| `lastCheckpointVersion` | `long` | Version of last checkpoint (-1 if none) |
| `lastCrcVersion` | `long` | Version of last CRC file (-1 if none) |
| `numCheckpointFiles` | `int` | Number of checkpoint files |

### DataLayoutInfo

| Field | Type | Description |
|-------|------|-------------|
| `numClusteringColumns` | `int` | Clustering columns (always 0 in OSS Delta) |
| `numPartitionColumns` | `int` | Partition columns |
| `numDistinctPartitions` | `long` | Distinct partition values |

### Example

```json
{
  "name": "basic_read_tbl",
  "description": "basic_read — tbl",
  "schema": {
    "type": "struct",
    "fields": [
      { "name": "id", "type": "integer", "nullable": false, "metadata": {} },
      { "name": "name", "type": "string", "nullable": true, "metadata": {} }
    ]
  },
  "protocol": {
    "minReaderVersion": 1,
    "minWriterVersion": 2
  },
  "logInfo": {
    "numAddFiles": 1,
    "numRemoveFiles": 0,
    "sizeInBytes": 534,
    "numCommits": 2,
    "numActions": 4,
    "lastCheckpointVersion": -1,
    "lastCrcVersion": 1,
    "numCheckpointFiles": 0
  },
  "properties": {},
  "dataLayout": {
    "numClusteringColumns": 0,
    "numPartitionColumns": 0,
    "numDistinctPartitions": 0
  },
  "tags": ["basic", "read"]
}
```

On failure, a minimal sentinel is written:

```json
{
  "name": "failed_test",
  "description": "test that failed during generation",
  "error": "Metadata scan failed: table not found"
}
```

---

## Expected Data Layout

Each workload output directory has this structure:

```
<test_name>/
├── delta/                          # The Delta table itself
│   └── _delta_log/
│       ├── 00000000000000000000.json
│       ├── 00000000000000000001.json
│       └── ...
├── specs/                          # Spec JSON files
│   ├── <test>_read.json
│   ├── <test>_read_v0.json
│   ├── <test>_snapshot.json
│   ├── <test>_read_v0.json
│   └── ...
├── expected/                       # Expected data per spec
│   ├── <test>_read/
│   │   ├── expected_data/          # Parquet: expected rows
│   │   └── expected_metadata/      # Parquet: AddFile actions scanned
│   ├── <test>_read_v0/
│   │   ├── expected_data/
│   │   └── expected_metadata/
│   └── <test>_read_v0/
│       ├── expected_data/
│       └── expected_metadata/
├── table_info.json                 # Table metadata
└── repro/
    └── generate.scala              # Script to reproduce this workload
```

### Naming Conventions

Spec files and expected directories share names derived from the test and spec parameters:

| API Call | Spec File Name |
|----------|---------------|
| `readSpec(t)` | `<test>_read.json` |
| `readSpec(t, version=0)` | `<test>_read_v0.json` |
| `readSpec(t, predicate="id > 5")` | `<test>_read_id_gt_5.json` |
| `readSpec(t, columns=Seq("id"))` | `<test>_read_cols_id.json` |
| `snapshotSpec(t)` | `<test>_snapshot.json` |
| `snapshotSpec(t, version=2)` | `<test>_snapshot_v2.json` |
