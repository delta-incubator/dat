# Contributing

## Getting started

This repository uses `poetry` to handle Python dependencies and an isolated environment. To install the environment, run

```
poetry install
```

Most common tasks are defined in the Makefile. For example, to lint the Python files run

```
make lint
```

To open a shell session within the poetry environment, use

```
poetry shell
```

## Generating reference tables locally

Reader test cases are defined in `dat/generated_tables.py`. Currently, they are all defined in terms of PySpark code. If there is a table that doesn't fit into that paradigm, create an issue on the repository and we will consider other modes of adding tables.

To regenerate tables, run

```
make write-generated-tables
``` 

## Adding writer tests

TBD

## Running unit tests

You can run the unit tests with `poetry run pytest tests`.

If you've already run `poetry shell`, you can run the tests with `pytest tests`.

