# Workload-generator architecture restructure (Level B)

Goal: reorganize the workload-generator into clean, single-responsibility layers so each module has
one reason to change. Pure structural moves + the cohesion fixes already identified — **no behavior
change**. Verified green at each step; full-generation gate before any push.

Guiding principle: manage complexity. The on-disk **spec model** is the center; producers (capture)
and consumers (validate) depend on it but not on each other. Each layer transforms the abstraction;
the write feature layers cleanly on top of the read/snapshot foundation.

## Stack allocation

- **PR1 (`stack/typed-engine-foundation`)** — everything non-write: the model/json/engine/capture/
  validate/log/SPI/DSL layers for `read` + `snapshot`.
- **PR2 (`stack/write-on-foundation`)** — the write layer only: write model + write serde, the
  `write/` package (builder + replay + SQL), the write SPI methods, and the write hunks of the
  shared DSL/validator/generator files.

## Target package layout

```
io.delta.workload
├── model/        on-disk contract, pure data (NO logic, NO mapper)
│                 PR1: SpecError, ReadResult, SnapshotResult, Spec/ReadSpec/SnapshotSpec,
│                      SpecExpectation(+Succeeded/Failed), ErrorExpectation,
│                      ProtocolInfo, MetadataInfo, LogInfo, DataLayoutInfo, TableInfo
│                 PR2: WriteSpec + the WriteCommit ADT (Create/Replace/Insert/Delete/Update/
│                      EvolveSchema/UpdateProperties/LowLevelCommitOp + low-level action types)
├── json/         serde ONLY — the mapper, the (envelope-shared) Spec serializers/deserializers,
│                 StructType serde, writeSpec/readSpec/read*Spec
│                 PR1: read/snapshot serde.  PR2: WriteSpec serde + readWriteSpec.
├── engine/       read+compare engine (was SpecSupport)
│                 resolveSnapshot/buildDeltaReader/applyFilters, captureExpectation/
│                 assertExpectation/compareExpectation/runErrorCode, assertRowsEqual/canonicalize,
│                 formatTimestamp/parseTimestamp, extractErrorCode/normalizeErrorCode   (PR1)
├── capture/      artifact producers — ReadCapture, SnapshotCapture, TableInfoWriter   (PR1)
├── write/        write-spec round-trip (PR2) — WriteSpecBuilder (build) + WriteReplay
│                 (replay, EXTRACTED from WorkloadValidator) + TableSql + SpecLayout
├── validate/     WorkloadValidator (thin: walk dirs, pick captured-vs-replayed, dispatch)
│                 + ValidationResult/SpecOutcome   (PR1 read/snapshot dispatch; PR2 adds write case)
├── deltaharness/ engine SPI (existing; PR2 adds commit/CommitRequest/writeRows/schemaAt)
├── log/          typed Delta log — Action, CommitLog, CommitInfo; move LastCheckpointInfo here
│                 (it is a log artifact, currently mis-homed in JsonUtil)   (PR1)
└── (top level)   DSL + orchestration — WorkloadOps, WorkloadContext, TableHandle,
                  SpecDeclarations, WorkloadGenerator, WorkloadTestSuite
                  (PR1 read/snapshot; PR2 adds write DSL methods + write-builder state + baseline emit)
```

## Cohesion fixes folded in
- **#1 Split `JsonUtil` (492 LOC)** → `model/` (data) + `json/` (serde). Model keeps its Jackson
  annotations; the serializer classes + mapper move to `json/`.
- **#2 Extract write replay** from `WorkloadValidator` → `write/WriteReplay.scala`, co-located with
  `WriteSpecBuilder` (build + replay of a WriteSpec sit together; validator becomes a thin dispatcher).
- **Note (keep):** `WorkloadOps` pass-throughs over `WorkloadContext` — defensible (the mixin gives
  test authors the unqualified DSL surface). Leave as-is.
- **Note (later):** if `engine/` grows, split `assertRowsEqual`/`canonicalize` into `RowComparison`.
- **Already done this session:** serde envelope unify (`writeEnvelope` + `SpecSerializer`/
  `SpecDeserializer` bases), capture error-handling unify (`captureExpectation`/`assertExpectation`/
  `runErrorCode`, `NonFatal`), `assertMatches` `requireEq` dedupe, `read_all` default.

## Workflow (avoid the per-change reparent churn)
Do the whole restructure on a **single integration branch** built from the full combined tree
(current PR2 tip `f3d8c915`, which already has write + the serde/assertMatches work), get it fully
green, then **derive the two PRs once** at the end:
- PR1 = restructured tree **minus** the `write/` package and the write hunks.
- PR2 = the write layer on top of PR1.

This also corrects the current inconsistency (the serde+assertMatches change presently sits on PR2's
branch; the rebuild places the non-write parts in PR1 where they belong).

## Execution steps
1. Integration branch from `f3d8c915` (full tree). Confirm it compiles + a smoke suite is green.
2. Create packages and move files, one layer at a time, compiling after each:
   `model/` → `json/` → `log/` (LastCheckpointInfo) → `engine/` → `capture/` → `validate/` →
   `write/` (incl. WriteReplay extraction) → fix top-level DSL imports.
3. Update `package`/`import` lines; no logic edits. `Test/compile` green after every layer.
4. Full targeted test sweep (read/snapshot suites + write suites + WorkloadGeneratorSuite + Corruption).
5. Derive PR1 (non-write) and PR2 (write) from the integration branch.
6. Per-PR gate: PR1 compiles + read/snapshot suites green standalone; PR2 compiles + write suites green.
7. Full-generation batch gate, then re-derive stack metadata and force-push (updates #58/#59).

## Verification gates (hard rule: never push without compile + tests green)
- `Test/compile` after each layer move.
- Targeted suites after the moves (fast feedback).
- Full generation (`sbt test`) as the pre-push batch gate, per PR.

## Risks / rollback
- Big import churn → compile-after-each-layer catches breakage early.
- Backups: `backup/golden-write-on-foundation`, branch reflogs, and `f3d8c915` (full tree) all
  preserve the pre-restructure state. Any step is revertable.
- On-disk spec JSON must not change (pure moves) — WorkloadGeneratorSuite asserts spec shape;
  the full-gen gate confirms byte-stable output.
