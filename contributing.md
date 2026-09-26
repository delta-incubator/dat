# Contributing

## Getting started

The workload generator is an sbt project. To compile it, run

```
cd workload-generator
sbt Test/compile
```

Most common tasks are defined in the justfile. For example, to run tests:

```
just test
```

## Generating workloads locally

Workload suites are defined under `workload-generator/src/test/scala/io/delta/workload/tables`.
See `workload-generator/docs/authoring-guide.md` for authoring and generation instructions.

## Adding writer tests

TBD

## Running unit tests

Run `just test`, or use the focused sbt commands documented in `workload-generator/README.md`.
