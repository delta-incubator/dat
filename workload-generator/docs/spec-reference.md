# Spec Format Reference

This document defines every spec type produced by the workload generator. Each spec is a self-contained JSON file describing an operation to execute against a Delta table and the expected result. Specs live in the `specs/` directory of each workload output.

All specs follow the same top-level pattern:

```json
{
  "type": "<spec_type>",
  ...operation parameters...,
  "expected": { ...success expectations... },
  "error": { "errorCode": "...", "errorMessage": "..." }
}
```

Exactly one of `expected` or `error` is present. The other is omitted (not `null`).

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
| `error` | `SpecError` | no | Present on expected failure |

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

- **`expected_data/`** — Parquet files with the exact rows the read should return. Row order is irrelevant; comparison is a typed row bag (schemas must match by name and type; each row appears the correct number of times). Capped at 5,000,000 rows.
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
  "error": {
    "errorCode": "DELTA_VERSION_NOT_FOUND",
    "errorMessage": "Cannot find version 999"
  }
}
```

**Error: unsupported protocol:**

```json
{
  "type": "read",
  "error": {
    "errorCode": "DELTA_UNSUPPORTED_FEATURES_FOR_READ",
    "errorMessage": "Required reader features not supported: [futureFeature]"
  }
}
```

**Error: corrupt data file:**

```json
{
  "type": "read",
  "error": {
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
| `error` | `SpecError` | no | Present on expected failure |

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
  "error": {
    "errorCode": "DELTA_UNSUPPORTED_FEATURES_FOR_READ",
    "errorMessage": "Table requires reader feature 'unknownFeature' which is not supported"
  }
}
```

---

## Write Spec

**Type:** `"write"`

Tests Delta *writer* implementations: a portable, engine-agnostic recipe of `commits` to replay into a fresh table.

It is **just another spec** — it lives at `specs/<name>_write.json` and dispatches by `type`. Its presence makes the whole directory *write-derived*: every read/snapshot spec there is validated against the table replayed from the write spec (not the captured `delta/` table). No per-spec pointer is needed; the decision is per directory.

The write spec's own validation is **basic**: the replay must succeed and the final version must equal `commits.size - 1` (each commit advances the table by exactly one version). It carries no expected-rows artifact of its own. The final-state rows are checked by an auto-generated baseline `latest` read spec (see [Expected Data](#expected-data-1) below); per-version protocol/metadata by the snapshot spec(s).

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
| `create_table` | `schema`, `partitionColumns?`, `properties?` | Create a new table with the given schema (`schema` is a Delta-JSON struct) |
| `replace_table` | `schema`, `partitionColumns?`, `properties?`, `dataFiles?` | Replace the table's schema/partitioning/properties (and all data). With `dataFiles` it is a replace-as-select: a single commit that also writes the bundled data. |
| `insert` | `dataFiles?` | Append the rows in the bundled Parquet data files |
| `update` | `predicate`, `set` | Update rows matching `predicate`; `set` maps column → SQL expression |
| `delete` | `predicate` | Delete rows matching `predicate` (SQL WHERE clause) |
| `evolve_schema` | `addColumns?`, `renameColumns?`, `dropColumns?` | Modify table schema. `addColumns` entries are `{name, type, nullable}`; `renameColumns` maps old → new |
| `update_properties` | `set?`, `remove?` | Modify table properties (`set` map, `remove` names) |

The data for `insert` and `replace_table` is bundled as Parquet under `data/commit_N/` (the
generator's authoring API accepts in-memory rows, but the spec always stores Parquet). A
consumer replays by reading those files — `insert` appends them; `replace_table` performs a
single replace-as-select from them.

`RENAME COLUMN` / `DROP COLUMN` in `evolve_schema` require the table to have column mapping
enabled (a Delta/Spark constraint).

#### Low-Level Operation

| Operation | Fields | Description |
|-----------|--------|-------------|
| `commit` | `schema?`, `tableProperties?`, `txn?`, `addFiles?`, `removeFiles?`, `addDomainMetadata?`, `removeDomainMetadata?` | Bundle raw Delta actions in one commit. Each `addFiles[]` entry's logical rows are bundled as Parquet (`dataFile`); a consumer writes them **through its own engine write path** (so column mapping, partitioning, and stats are handled — physical names/stats are not in the spec) and commits the resulting `AddFile`s alongside the `txn`/`DomainMetadata`/schema/property changes. `removeFiles[]` tombstone a file added by a prior low-level `commit`, referenced by that commit's ordinal (`addedAtCommit`); since the engine assigns paths per table, the consumer resolves the ordinal to its own table's path. Deletion vectors are out of scope. |

### Low-level value types

Value types referenced by the low-level `commit` operation. An `AddFileAction` carries a pointer to a Parquet of **logical** rows (full rows incl. partition columns); the consumer writes it through its own engine write path, so physical names, partitioning, and stats are derived per table, not stored.

| Type | Field | Description |
|------|-------|-------------|
| `AddFileAction` | `dataFile` | Relative path under `data/commit_N/` to the Parquet of logical rows |
| `AddFileAction` | `dataChange?` | Whether this is a data change (default true) |
| `RemoveFileAction` | `addedAtCommit` | Ordinal (== table version) of the prior low-level `commit` that added the file(s) to tombstone; consumer resolves it to its own table's path(s) |
| `RemoveFileAction` | `dataChange?` | Whether this is a data change (default true) |
| `AppTxn` | `appId`, `version` | Application id and its monotonic transaction version for idempotent writes (`txn` action) |
| `AddDomainMetadata` | `domain`, `configuration` | Domain name and its configuration payload |

### Expected Data

A write spec has **no expected-data artifact of its own**. The final table state is captured by an auto-generated baseline read spec named `latest` (file `specs/<name>_latest.json`, expected rows under `expected/<name>_latest/expected_data/`), which the generator emits for every write workload. That read is validated against the replayed table, so the final-state rows become consumer-validatable just like any other read.

### Comparison Semantics

The capture comes from one writer, so a consumer compares the replayed table against expectations *portably*, not byte-for-byte:

- **rows** — checked by the baseline `latest` read spec: typed row equality, order-independent bag (schema must match by name and type).
- **protocol** — checked by the snapshot spec(s): the replay must support at least the expected reader/writer versions and features (a stronger protocol is acceptable).
- **configuration** — only the keys the spec's own commits declared: `create_table`/`replace_table` `properties` + `update_properties.set` minus `.remove` (`replace_table` resets the set); each present and equal, removed keys absent. Engine-injected defaults are ignored.
- **schemaString** — equal with per-field column-mapping `physicalName`/`id` normalized out (minted per table); `partitionColumns` and `format` equal.

The write spec itself only asserts that the replay succeeds and produces the expected number of versions (`finalVersion == commits.size - 1`).

### Directory Structure

```
<test_name>/
├── delta/                       # the captured table (for read-only validation)
├── specs/
│   ├── <test>_read*.json
│   ├── <test>_latest.json       # auto baseline read of the final state
│   ├── <test>_snapshot.json
│   └── <test>_write.json        # the write spec — just another spec
├── data/                        # Parquet referenced by write commits
│   ├── commit_1/
│   │   └── part-00000.parquet
│   └── ...
├── expected/
│   ├── <test>_latest/           # final-state rows (baseline read)
│   │   └── expected_data/
│   └── <read_spec_name>/        # expected_data for each read spec
│       └── expected_data/
└── table_info.json
```

### Examples

**High-level sequence (create, insert, delete, update):**

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
          { "name": "status", "type": "string", "nullable": true, "metadata": {} }
        ]
      },
      "properties": { "delta.enableDeletionVectors": "true" }
    },
    { "operation": "insert", "dataFiles": ["data/commit_1/part-0000-abc.parquet"] },
    { "operation": "delete", "predicate": "id > 100" },
    {
      "operation": "update",
      "predicate": "status = 'pending'",
      "set": { "status": "'active'" }
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

**Replace-as-select (replace schema + data in one commit):**

```json
{
  "type": "write",
  "commits": [
    { "operation": "create_table", "schema": { } },
    { "operation": "insert", "dataFiles": ["data/commit_1/part-00000.parquet"] },
    {
      "operation": "replace_table",
      "schema": {
        "type": "struct",
        "fields": [
          { "name": "id", "type": "integer", "nullable": true, "metadata": {} },
          { "name": "label", "type": "string", "nullable": true, "metadata": {} }
        ]
      },
      "dataFiles": ["data/commit_2/part-00000.parquet"]
    }
  ]
}
```

**Low-level commit (addFiles/removeFiles + txn + domainMetadata):**

```json
{
  "type": "write",
  "commits": [
    {
      "operation": "create_table",
      "schema": { },
      "properties": { "delta.feature.domainMetadata": "supported" }
    },
    { "operation": "commit", "addFiles": [ { "dataFile": "data/commit_1/add_0.parquet" } ] },
    {
      "operation": "commit",
      "txn": { "appId": "streaming-job-1", "version": 42 },
      "addFiles": [ { "dataFile": "data/commit_2/add_0.parquet" } ],
      "removeFiles": [ { "addedAtCommit": 1 } ],
      "addDomainMetadata": [
        { "domain": "myApp.config", "configuration": "{\"version\": 1}" }
      ]
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
