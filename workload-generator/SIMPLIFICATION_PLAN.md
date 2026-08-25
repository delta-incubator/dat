# Workload-Generator Architecture & Simplification Plan (dat)

> Scratch/planning doc (untracked). Lens: software-design-advisor (manage complexity, deep modules,
> information hiding) + "use what Spark already has". **Two-PR plan** — a foundation PR that types the
> engine boundary and the query specs, then the write *generator* rebased onto it.

## Diagnosis — 3 root causes

Almost every flagged item is a symptom of one of these:

- **A. Schema has 4 representations** — DDL → `StructType` → `StructType.json` → untyped `Any` map.
  The last hop spawns `ddlToSchemaJson`, `structOf`, `addColumnsJson`, the `asInstanceOf` field-loop.
- **B. Capture and replay are parallel hand-rolled paths sharing no helpers** — duplicated SQL clause
  builders, JSON canonicalization split across files, the multi-ALTER `EvolveSchemaCommit` bug.
- **C. Validator result + dispatch is ad-hoc** — `passed: Int`/`errors` counter, parallel read-only
  vs write-derived branches, per-spec `writeSpec` pointer that's uniform per dir.

**Cross-cutting principle: strong types over JSON/string munging.** Every place we `readTree` a node,
match `node.get("type")` as a string, strip fields from a JSON tree, rebuild a path from a filename,
or store schema as `Any` is a symptom. Parse to a strong type once at the boundary, operate on the
type, serialize once at the edge.

---

## Architecture — how it all fits together

**`Spec`** is the sealed umbrella over every spec file (`ReadSpec | SnapshotSpec | WriteSpec`). Each
carries an optional expectation and is **runnable** — `run(spec, table)` produces its `Result`. Two
*kinds* of run:

- **query** (`Read` | `Snapshot`) — *reads* the table → rows / snapshot;
- **write** (`WriteSpec`) — *replays* its commits *into* the table → final version (basic result).

A **`SpecFile`** pairs the file `path` (→ name, `expected/` dir) with the parsed `Spec`.

```
                         DeltaHarness (the ONLY thing that touches Spark/Delta)
                         ────────────────────────────────────────────────────
                         snapshotAt(table, version, timestamp): Snapshot{ protocol, metadata{ id, schema,
                                              config, partitionCols, format, createdTime } }   ◄── typed, FULL, not JSON
                         read(table, version, timestamp, predicate, columns): DataFrame
                         replay(commits): table          allFiles / commit / openLog
                              ▲                     ▲
                   capture    │ run(spec, table)    │ replay(commits)
                              │                     │
        ┌──────────────────────────────────────────────────────┐
        │ Spec = ReadSpec | SnapshotSpec | WriteSpec             │  all runnable; each has an optional
        │   query specs READ the table; WriteSpec REPLAYS into it │  expectation
        └─────────────────────────┬──────────────────────────────┘
                                  │
   validate(SpecFile, table): SpecOutcome   ── target = captured `delta/`  OR  the table WriteSpec replayed into
   compareEither(spec.expectation, run(spec, target))
   ValidationResult = Seq[SpecOutcome]
```

Three facts make it collapse:

**1. Capture and Validate are inverses over the *same* `(engine, Spec)`.** Capture runs the spec on a
table and stores the outcome as its `expectation`; Validate runs it again and compares. The table is a
**parameter** — "captured vs replayed" is *where the table came from*, not two code paths.

**2. The write spec runs, validates, AND produces the table.** Running it replays its commits into a
fresh table; **basic validation** = replay succeeds + `finalVersion == commits.size - 1` (the
commit-index==version invariant), plus the expect-error case. That replayed table is then the target the
read/snapshot specs validate against — and the generator emits a baseline unfiltered-read + latest-snapshot
for the deeper row/snapshot checks. So there's **no** `table_content` or `validateExpectedLatest`: the
write spec's own check is the cheap version-count; the rich checks are the baseline query specs.

**3. The cruft all traces to one boundary leak: the engine returns JSON, not types.** `SnapshotView`
hands back `protocolJson`/`metadataJson` strings, so every consumer re-parses + re-normalizes. That
single leak is the root of **A** (schema as `Any`), **B** (`stripColumnMapping` etc. — you strip
volatile fields only because you diff *blobs* not *types*), and the snapshot-validator fork.

**The Spec/Result dual.** Each `Spec` fixes its own `Result`; one `run` maps a spec to
`Either[SpecError, Result]`; one `Compare` checks two results; one `compareEither` is the verdict over
the spec's stored expectation vs a fresh run. `run` reads only the request fields and **ignores the
expectation**, so a spec is runnable at capture (no expectation yet) and at validate alike:
- capture: `run(spec, captured)` → store as the spec's `expectation`;
- validate: `compareEither(spec.expectation, run(spec, target))`.

`expectation: Option[Either[SpecError, Result]]` is one field — `None` until captured, then `Right`
(success) or `Left` (error) — replacing the parallel `expected`/`expectedError` Options. So the error
path falls out of `compareEither` instead of a special branch, and there is **no separate config type**
(a spec already carries its request fields; this matches today's `SnapshotSpec`/`ReadSpec` *and* the
kernel Rust spec).

**The keystone:** push typing *down into the harness*. `DeltaSparkSnapshotView` already wraps a Delta
`Snapshot` with `.protocol` and `.metadata.schema: StructType` — it's currently down-converting to JSON
and making callers convert back up. Invert it: expose a neutral typed `Snapshot`. Then capture = run a
spec on a typed snapshot; validate = compare; write = replay → same validate.

**Two PRs:** **PR 1** = the typed boundary + the query specs (`Read`/`Snapshot`) end-to-end — a pure
refactor of #54 code. **PR 2** = the `WriteSpec` generator + replay, rebased on top.

---

## Cruft scorecard — what the design kills

Verified against call sites. **Every schema-munging helper and metadata "normalization" dies**, incl.
`canonicalJson` (its only callers are the snapshot diffs, which go typed).

| Helper / normalization | Fate |
|---|---|
| `canonicalJson`, `ddlToSchemaJson`, `structOf`, `addColumnsJson` | **DELETED** — typed `StructType` serde + typed compare |
| `replayEvolveSchema` `asInstanceOf` casts | **DELETED** — typed `StructType.toDDL` |
| `stripColumnMapping`, `normalizedSchema`, `canonicalField` | **DELETED** — logical typed `StructField` compare |
| `configurationViolation` / `declaredConfiguration` / `ConfigScope` | **DELETED** — `configuration: Map` compared exactly (`==`) |
| `readTree(...).get("metaData")` re-parse (every consumer) | **DELETED** — typed `harness.snapshotAt(...)` |
| `toRowMultiset` / `assertMultisetsEqual` (JSON multiset) | **DELETED** — `schema ==` + Spark `exceptAll` (maps canonicalized, not dropped) |
| `validateExpectedLatest` + `writeExpectedLatest` / `table_content` | **DELETED** — write's own check is basic (replay + version count); rows/snapshot are the baseline query specs over the replay |
| two snapshot validators | **UNIFIED** — one `Compare[SnapshotResult]` |
| `passed:Int`/`errors` counter; `expected`+`expectedError` Options; `*SpecConfig` | **REPLACED/FOLDED** — `SpecOutcome`; `expectation: Option[Either]` on the one `Spec` |
| per-spec `writeSpec` pointer; string `node.get("type")` + `stripSuffix` | **DELETED/REPLACED** — `WriteSpec` is the table source; `@JsonTypeInfo` discriminator + `SpecFile` |
| `record`'s `Int` return; `WorkloadResult.{readSpecs,snapshotSpecs}` | **REMOVED** — dead |

**Survivors — irreducible non-reproducibility (typed, never blob-stripping):**

| Survivor | Why intrinsic |
|---|---|
| CM physical names excluded from schema compare | minted per table; replay can't reproduce them. Typed `StructField` compare minus the 2 CM metadata keys (same category as not putting `id`/`createdTime` in `SnapshotResult`). |
| read-side **file-pruning** check, captured-table only | *which* files a scan pruned is a property of the **physical** file set; meaningless vs a replay. The one read-only-only assertion, kept *outside* the generic `Compare`. |

After this: **no JSON comparison, no config scope.** The only exclusions are facts about what an
independently-built table cannot reproduce — in typed terms, not blob normalization.

---

## End-to-end: the model

```scala
// `Spec` is the sealed umbrella over every file in specs/. One `type` discriminator (Jackson, like the
// WriteCommit ADT) parses a file into one of the three. EVERY spec is runnable and carries an OPTIONAL
// expectation (None until captured) — `run` produces its Result against the table-in-question.
@JsonTypeInfo(use = Id.NAME, property = "type")
@JsonSubTypes(Array(new Type(classOf[SnapshotSpec], name = "snapshot"),
                    new Type(classOf[ReadSpec],     name = "read"),
                    new Type(classOf[WriteSpec],    name = "write")))
sealed trait Spec {
  type Result
  def expectation: Option[Either[SpecError, Result]]
  def withExpectation(e: Either[SpecError, Result]): Spec   // capture fills it (per-case `copy`)
}

// A parsed spec file = its path (→ name + expected/ dir) + the parsed Spec.
final case class SpecFile(path: Path, spec: Spec) {
  def name = path.getFileName.toString.stripSuffix(".json")
  def expectedDir(testDir: Path) = testDir.resolve("expected").resolve(name)
}

// `run` is UNIFORM — a read/snapshot spec READS the table; a write spec REPLAYS its commits INTO it.
trait Runner[S <: Spec] { def run(spec: S, table: Path): Either[SpecError, S#Result] }
trait Compare[R]        { def check(expected: R, actual: R): Unit }
def run[S <: Spec : Runner](spec: S, table: Path) = implicitly[Runner[S]].run(spec, table)

// the verdict layer — both success and error handled in one place:
def compareEither[R](expected: Either[SpecError, R], actual: Either[SpecError, R])(cmp: Compare[R]): Unit =
  (expected, actual) match {
    case (Right(e), Right(a)) => cmp.check(e, a)                  // success matches
    case (Left(_),  Left(_))  => ()                              // error reproduced (code not matched)
    case (Left(e),  Right(_)) => fail(s"expected error ${e.code} but succeeded")
    case (Right(_), Left(a))  => fail(s"expected success but failed: ${a.message}")
  }

def capture[S <: Spec : Runner](spec: S, table: Path): Spec = spec.withExpectation(run(spec, table))  // None -> Some
// ONE validate for read, snapshot, AND write — compare the stored expectation to a fresh run:
def validate(sf: SpecFile, table: Path): SpecOutcome = outcome(sf.name) {
  sf.spec match {
    case s: SnapshotSpec => compareEither(s.expectation.get, run(s, table))(snapshotCompare)
    case s: ReadSpec     => compareEither(s.expectation.get, run(s, table))(readCompare)
    case s: WriteSpec    => compareEither(s.expectation.get, run(s, table))(writeCompare)  // BASIC: replay + version count
  }
}
```

**Snapshot instance** — `SnapshotSpec#Result = SnapshotResult`:

```scala
// version/timestamp are the EXISTING on-disk fields (the kernel reads them) — kept verbatim; the
// version⊕timestamp XOR stays a runtime `require` (not changed to a typed selector — that's on-disk).
case class SnapshotSpec(version: Option[Long] = None, timestamp: Option[String] = None,
                        expectation: Option[Either[SpecError, SnapshotResult]] = None) extends Spec {
  type Result = SnapshotResult; def withExpectation(e) = copy(expectation = Some(e)) }

def snapshotAt(spark: SparkSession, table: Path, version: Option[Long], timestamp: Option[String]): Snapshot

implicit val snapshotRunner: Runner[SnapshotSpec] = (s, t) =>
  try Right(SnapshotResult.from(snapshotAt(spark, t, s.version, s.timestamp))) catch { case NonFatal(e) => Left(SpecError.of(e)) }

// SnapshotResult carries the FULL metadata, so on disk it serializes to the kernel's {protocol, metadata}.
case class SnapshotResult(protocol: ProtocolInfo, metadata: Metadata)  // metadata: id, format, schema, partCols, createdTime, config

// the ONE Scala snapshot comparator — LOGICAL, used for both read-only and replay (the kernel does its own FULL equality):
implicit val snapshotCompare: Compare[SnapshotResult] = (e, a) => {
  requireEqual(e.protocol, a.protocol)
  requireEqual(logicalSchema(e.metadata.schema), logicalSchema(a.metadata.schema))   // nested, minus the 2 CM keys
  requireEqual(e.metadata.partitionColumns, a.metadata.partitionColumns)
  requireEqual(e.metadata.configuration, a.metadata.configuration)                   // typed Map == (id/createdTime/CM excluded — not reproducible)
}
```

**Read instance** — `ReadResult` is a **summary** on the wire; its rows live on disk and are **read back
at parse time** so the expected side is complete before `validate`:

```scala
case class ReadSpec(version: Option[Long] = None, timestamp: Option[String] = None,
                    predicate: Option[String] = None, columns: Option[Seq[String]] = None,
                    expectation: Option[Either[SpecError, ReadResult]] = None) extends Spec {
  type Result = ReadResult; def withExpectation(e) = copy(expectation = Some(e)) }

// serialized = counts only; `rows` is transient (expected: hydrated from expected_data/; actual: the live read).
case class ReadResult(rowCount: Long, fileCount: Int, filesSkipped: Long, @JsonIgnore rows: DataFrame)

implicit val readRunner: Runner[ReadSpec] = (s, t) =>
  attempt(ReadResult.of(harness.read(t, s.version, s.timestamp, s.predicate, s.columns)))
implicit val readCompare: Compare[ReadResult] = (e, a) => {
  requireEqual(e.rowCount, a.rowCount)
  assertRowsEqual(e.rows, a.rows, "read")        // e.rows hydrated from expected_data/ at parse (typed diff, below)
}
```

**Write instance** — `WriteSpec.run` **replays** its commits into the table; basic validation = version count:

```scala
case class WriteSpec(commits: Seq[WriteCommit],
                     expectation: Option[Either[SpecError, WriteResult]] = None) extends Spec {
  type Result = WriteResult; def withExpectation(e) = copy(expectation = Some(e)) }
case class WriteResult(finalVersion: Long)              // the load-bearing commit-index == version invariant

implicit val writeRunner: Runner[WriteSpec] = (s, t) =>             // REPLAYS into t (side effect: populates the table)
  attempt { replay(s.commits, into = t); WriteResult(latestVersion(t)) }
implicit val writeCompare: Compare[WriteResult] = (e, a) => requireEqual(e.finalVersion, a.finalVersion)
// capture sets expectation = Right(WriteResult(commits.size - 1)); a write meant to fail captures Left(error).
```

**Loading hydrates the on-disk read rows** (the only place that touches `expected_data/`):

```scala
def parse(path: Path, testDir: Path): SpecFile = JsonUtil.read[Spec](path) match {
  case r: ReadSpec =>                                                // counts deserialized; rows still empty
    val rows = spark.read.parquet(testDir.resolve("expected").resolve(name(path)).resolve("expected_data").toString)
    SpecFile(path, r.copy(expectation = r.expectation.map(_.map(_.copy(rows = rows)))))
  case other => SpecFile(path, other)                               // snapshot + write expectations are inline
}
```

### Capture, before/after (snapshot)

**Today** — JSON poking, `(Any, Any)` blob:
```scala
val protocol = mapper.treeToValue(mapper.readTree(snapshot.protocolJson).get("protocol"), classOf[Any])
val metadata = mapper.treeToValue(mapper.readTree(snapshot.metadataJson).get("metaData"), classOf[Any])
SnapshotSpec(writeSpec, version, timestamp, Some(SnapshotExpected(protocol, metadata)), None)
```
**After** — the existing `SnapshotCapture.capture` (driven by the `snapshotSpec(...)` DSL):
```scala
val spec = capture(SnapshotSpec(version, timestamp), table)   // run -> fill the spec's expectation (was None)
JsonUtil.writeSpec(specsDir.resolve(s"${testId}_snapshot.json"), spec)
```

### Validation dispatch — the table is a parameter; the write spec both validates AND produces it

```scala
val loaded    = listSpecs(specsDir).map(parse(_, testDir))                    // Seq[SpecFile]
val writeFile = loaded.find(_.spec.isInstanceOf[WriteSpec])
val target    = writeFile.map(_ => freshReplayTable()).getOrElse(capturedDelta)  // queries read this; write replays INTO it

// run the write spec FIRST — its `run` replays into `target` (basic-validating it); then queries read `target`:
val writeOut = writeFile.map(sf => validate(sf, target)).toSeq                // replay succeeds + finalVersion == commits-1
val queryOut = loaded.filterNot(_.spec.isInstanceOf[WriteSpec]).map(validate(_, target))
val pruning  = if (target != capturedDelta) Nil                              // file-pruning: captured-table only
               else loaded.collect { case sf if sf.spec.isInstanceOf[ReadSpec] => assertFilePruning(sf, target) }
ValidationResult(writeOut ++ queryOut ++ pruning)
```

Every spec goes through `validate` — including the write spec (basic: replay + version count). The write
spec runs first because its `run` *populates* `target`, which the read/snapshot specs then read.
File-pruning stays out of the generic `Compare` (the one physical-table-specific assertion).

### Typed row compare (the `Compare[ReadResult]` core)

```scala
def assertRowsEqual(expected: DataFrame, actual: DataFrame, name: String): Unit = {
  require(expected.schema == actual.schema,                       // TYPE CHECK — catches Int/Long etc. JSON misses
    s"$name: schema mismatch\n  exp ${expected.schema.toDDL}\n  act ${actual.schema.toDDL}")
  val e = canonicalizeMaps(expected); val a = canonicalizeMaps(actual)
  val missing = e.exceptAll(a); val extra = a.exceptAll(e)        // Spark TYPED bag diff — no JSON conventions
  require(missing.isEmpty && extra.isEmpty,
    s"$name: ${missing.count} missing / ${extra.count} extra\n${missing.limit(5).collect().mkString("\n")}")
}
// MapType isn't set-op-comparable in Spark -> sort entries to a comparable array (lossless, NOT dropped)
private def canonicalizeMaps(df: DataFrame): DataFrame = df.select(df.schema.map { f =>
  f.dataType match { case _: MapType => array_sort(map_entries(col(f.name))).as(f.name); case _ => col(f.name) }
}: _*)
```

---

## On-disk representation — no unnecessary duplication

**Principle:** persist the *generative input* (a query spec's request; a `WriteSpec`'s commits + input
Parquet) and the *non-derivable ground-truth Result*; never persist what's reconstructible. The
Spec/Result dual adds **no** new files.

1. **`delta/` vs the `WriteSpec`.** A write workload's table is fully replayable from its commits +
   input Parquet, so a separate `delta/` duplicates every data file. *Target:* write workloads ship
   the `WriteSpec` only; query specs validate against the replay. *Bridge:* keep `delta/` until the
   kernel runner replays write specs ("write is coming later"), then drop it — flagged for removal.
2. **`table_content` is gone (Architecture fact 2).** A write workload's final state *is* the baseline
   unfiltered-read spec's `expected_data` — no separate `table_content` artifact.
3. **Schema / protocol / config once.** `table_info.json` is the table descriptor; a `SnapshotResult`
   is the per-version expectation; at the *latest* version they coincide — don't write both (the
   `WriteSpec`'s `create_table` already carries the schema). (We already deleted `table_version_metadata.json`.)
4. **Schema is NOT one on-disk form.** Nested object in `table_info.json` + write commits, but an
   **escaped JSON string** in snapshot `metadata.schemaString` (Delta's metadata format). The typed
   `StructType` serde emits the right form per slot — there is no single byte-identical schema form.

### Frozen on-disk contract (delta-kernel-rs parses this — match it exactly)

Verified against `delta-kernel-rs/benchmarks/src/models.rs` (no `deny_unknown_fields`, so extra fields
are tolerated; removed fields the kernel doesn't read — e.g. the `writeSpec` pointer — are safe). The
redesign is **Scala-internal**; it MUST emit this JSON unchanged.

```
<workload>/
├─ delta/                          # captured Delta table — kernel validates read/snapshot against this
├─ specs/
│  ├─ <name>_read.json             # type:read       — version/timestamp/predicate/columns + expected|error
│  ├─ <name>_snapshot.json         # type:snapshot   — version/timestamp + expected{protocol,metadata}|error
│  └─ <name>_write.json            # type:write      — generator (kernel can't parse yet — "write later")
├─ expected/<name>/expected_data/  # read rows (kernel asserts rowCount + these rows)
├─ data/commit_<n>/                # write input Parquet
└─ table_info.json                 # descriptor (benchmarks-only; acceptance runner ignores it)
```

`version`/`timestamp` are **flattened siblings** of `type` (both optional; absent = latest). The
expectation is `expected` XOR **`error`** (flattened, untagged; omitted while uncaptured).

```jsonc
// specs/foo_snapshot.json — kernel SnapshotConstructionSpec (also accepts type "snapshotConstruction")
{ "type": "snapshot",
  "version": 3,                                          // optional; omit version+timestamp for latest
  "expected": {                                          // protocol AND metadata — FULL equality
    "protocol": { "minReaderVersion": 1, "minWriterVersion": 2 },   // feature arrays OMITTED when none
    "metadata": {
      "id": "…", "format": { "provider": "parquet", "options": {} },
      "schemaString": "{\"type\":\"struct\",\"fields\":[…]}",       // ESCAPED JSON STRING (not an object)
      "partitionColumns": ["c"], "createdTime": 1234567890,
      "configuration": { "delta.appendOnly": "true" } } } }

// error case — flattened `error`, NOT `expectedError`:
{ "type": "snapshot", "version": 99, "error": { "errorCode": "DELTA_…", "errorMessage": "…" } }
```
```jsonc
// specs/foo_read.json — kernel ReadSpec. rowCount asserted; fileCount/filesSkipped read-but-ignored.
{ "type": "read", "predicate": "id > 5", "columns": ["id","v"],
  "expected": { "rowCount": 12, "fileCount": 2, "filesSkipped": 3 } }
// rows: expected/foo_read/expected_data/*.parquet   (hydrated at parse + typed-diffed at validate)
```
```jsonc
// specs/foo_write.json — WriteSpec (kernel rejects type:write until it adds a Write handler).
{ "type": "write",
  "commits": [ { "operation": "create_table", "schema": { … }, "properties": { … } },
               { "operation": "insert", "dataFiles": ["data/commit_1/part-00000.parquet"] } ],
  "expected": { "finalVersion": 1 } }       // basic: replay produces commits.size versions (or "error" if it should fail)
```

**Why the on-disk metadata stays FULL but the Scala compare is logical:** the **kernel** compares
`protocol`+`metadata` by **full equality** (its own code) — fine, because `expected.metadata` was captured
from that same table. The **Scala validator** uses one **logical** `snapshotCompare` for *both* read-only
and replay (typed schema minus CM names, config, partition cols; `id`/`createdTime`/CM excluded) — logical
passes for the captured case too (same table), and is the *only* thing that works for an independent
replay. So `SnapshotResult` is the in-memory typed view; on disk it always serializes to the **full**
`{protocol, metadata}` the kernel's full equality needs.

For a **write workload** the generator also emits the baseline query specs — *their* ground truth IS the
final state (no separate `table_content`): `select *` → `expected_data/`; latest snapshot → `expected`.

⚠️ **Discrepancy to reconcile:** the kernel reads the error field as **`error`** (`{errorCode,
errorMessage}`), but the current dat generator emits **`expectedError`** — so today's error specs may not
validate in the new kernel reader. Align the generator to `error`.

---

# PR 1 — Foundation: typed boundary + query specs

Pure refactor of existing read/snapshot/table_info code (no new feature) → merges to `main` first.

### 1.1 Typed `Snapshot` on the SPI
```scala
// FULL typed metadata (so the on-disk expected.metadata is complete for the kernel's full equality):
case class Metadata(id: String, name: Option[String], description: Option[String], format: Format,
                    schema: StructType, partitionColumns: Seq[String], createdTime: Option[Long],
                    configuration: Map[String, String])
case class Snapshot(version: Long, protocol: ProtocolInfo, metadata: Metadata)
trait SnapshotView { def snapshot: Snapshot; def allFiles: DataFrame }   // drop *Json getters; KEEP allFiles
// DeltaSparkSnapshotView: build from inner.protocol / inner.metadata (already typed) — stop emitting JSON
```

### 1.2 Migrate consumers; `snapshotAt` + result duals
`ReadCapture`, `SnapshotCapture`, `TableInfoWriter` drop `readTree(...).get("metaData")` and read off the
typed `Snapshot`. **`TableInfoWriter` keeps its `logInfo` logic** — `numRemoveFiles`/`numCommits`/
checkpoints come from `CommitLog` + the `_delta_log` listing, and `numAddFiles`/`sizeInBytes`/distinct
partitions from `allFiles`; only its protocol/schema/config extraction moves to the typed snapshot.
Rename `SnapshotExpected → SnapshotResult` (typed; was `(Any, Any)`) and `ReadExpected → ReadResult`.

### 1.3 The query model: `Spec` + `Runner`/`Compare` + `compareEither` (Read, Snapshot)
The generic core + the two instances above. The snapshot comparator becomes `Compare[SnapshotResult]`;
this deletes `stripColumnMapping`/`normalizedSchema`/`canonicalField`/`configurationViolation`.

### 1.4 Principled result
```scala
sealed trait SpecOutcome { def spec: String }
case class SpecPassed(spec: String) extends SpecOutcome
case class SpecFailed(spec: String, reason: String) extends SpecOutcome
case class ValidationResult(outcomes: Seq[SpecOutcome]) {
  def failures = outcomes.collect { case f: SpecFailed => f }
  def success  = failures.isEmpty                                 // derived
  def errors   = failures.map(f => s"${f.spec}: ${f.reason}")     // back-compat accessor
}
```

### 1.5 Typed dispatch
`parse(path, testDir): SpecFile` via `@JsonTypeInfo` (no `getOrElse("")` / `stripSuffix` munging); the
validate loop matches `sf.spec`. (`WriteSpec` parsing/handling arrives in PR 2 — in PR 1 the `@JsonSubTypes`
cover `read`/`snapshot` only.)

**PR 1 deletes:** `*Json` getters, read-path JSON re-parse, the canonicalization machinery, the JSON
row multiset. **Adds:** typed `Snapshot`, `SnapshotResult`/`ReadResult`, `Spec`/`Runner`/`Compare`/
`compareEither`, `SpecFile`, `SpecOutcome`, typed row compare.

---

# PR 2 — WriteSpec generator, rebased on PR 1

`WriteSpec` is the table generator; everything else reuses PR 1's query pipeline.

### 2.1 Typed schema in the write-commit ADT (root A)
```scala
object StructTypeModule extends SimpleModule {                    // register once
  addSerializer(classOf[StructType], (v, g, _) => g.writeTree(mapper.readTree(v.json)))
  addDeserializer(classOf[StructType], (p, _) => DataType.fromJson(p.readValueAsTree().toString).asInstanceOf[StructType])
}
case class CreateTableCommit(schema: StructType, ...)             // was Any -> deletes ddlToSchemaJson/structOf/addColumnsJson
```

### 2.2 `EvolveSchemaCommit` = one op = one commit (root A/B)
```scala
val ops = c.addColumns.size + c.renameColumns.map(_.size).getOrElse(0) + c.dropColumns.map(_.size).getOrElse(0)
require(ops == 1, s"EvolveSchemaCommit must be one schema op (got $ops); each ALTER is its own commit")
c.addColumns.foreach(st => spark.sql(s"ALTER TABLE $t ADD COLUMNS (${st.toDDL})"))
```

### 2.3 `WriteSpec` — runnable, basic-validated, and the table source (root C)
Add `WriteSpec` to the `@JsonTypeInfo` subtypes (`type:"write"`): commits + input Parquet + an optional
`WriteResult` expectation. Its `Runner` **replays into** the table; `validate` does **basic** validation
(replay succeeds + `finalVersion == commits.size - 1`, or the expect-error case). The replayed table is
the `target` the read/snapshot specs validate against, and the generator emits a baseline
unfiltered-read + latest-snapshot for the deeper checks. Deletes `validateExpectedLatest`,
`writeExpectedLatest`, `table_content`, the per-spec `writeSpec` pointer, `validateSnapshotDerived`.
*Verify engine-injected config is deterministic across table instances; if a key proves non-reproducible,
exclude it via one global volatile-keys constant — not a per-spec scope.*

### 2.4 Quick kills
```scala
def record(commit: WriteCommit, rows: Seq[Seq[Map[String, Any]]] = Nil): Unit = recorded += WriteOpWithData(commit, rows)
private def partitionedByClause(cols: Seq[String]) = if (cols.isEmpty) "" else s" PARTITIONED BY (${cols.mkString(", ")})"
private def tblPropertiesClause(p: Map[String,String]) = if (p.isEmpty) "" else s" TBLPROPERTIES (${p.map { case (k,v) => s"'$k' = '$v'" }.mkString(", ")})"
```
Drop `WorkloadResult.{readSpecs, snapshotSpecs}`.

### 2.5 Already pushed to #55 (carry forward)
`WriteOpWithData`, `commit` fold, `writeRowsToTemp` off SPI, `schemaAt` `includePartition` removed,
`writeExpectedLatest` trim, `AddFileInput` doc, `cleanupDir` deleted. (Staged batch held for the rebase.)

---

## Non-goals — keep apart

- **Rows compared typed, not via JSON** — `schema ==` + Spark `exceptAll` (maps canonicalized). Avoids
  type mismatches and JSON false-equality (`1` vs `1.0`, decimal scale, tz).
- **Don't collapse the directions** capture (`table → Spec`) vs validate (`Spec, table → Outcome`).
  They share the typed snapshot, serde, and comparators — not control flow. Replay is the arrow that
  produces `table`; *that* unifies, not capture⊕validate.
- **No separate request type** — a `Spec` *is* its request fields + an optional expectation (`None`
  until captured), matching today's `SnapshotSpec`/`ReadSpec` *and* the kernel Rust spec; `*SpecConfig`
  folds in and the DSL builds the `Spec` directly. Only the `WorkloadOps` forwarding facade + god-file
  split stay out of scope.
  - *Loose end:* `*SpecConfig` also carries the `HasAssertion` post-capture hook (`checkAssertion`).
    When it folds in, that hook needs a home — a field on `Spec` or handled in the DSL. Decide it; don't drop it.

---

## Sequencing & tests

- **PR 1**: 1.1 → 1.2 → 1.3/1.4/1.5. Gate on existing read/snapshot suites (`*DataSkippingSuite
  *WorkloadGeneratorSuite *CorruptionSuite`) — behavior unchanged.
- **PR 2** (rebased on merged PR 1): 2.1 → 2.2 → 2.3 → 2.4. Gate on `*WriteCommitSuite *WriteBasicSuite
  *WriteSequencesSuite *WriteTypesSuite *WorkloadValidatorSuite`.
- Per change: `cd workload-generator && JAVA_HOME=…17 WORKLOAD_FORCE=true sbt testOnly …`, then commit + ff-push (OSS account).

**Cross-repo follow-up (not here):** kernel `acceptance_workloads_reader` rejects `type:"write"` — needs
a `Write` handler when write support lands ("write is coming later").
