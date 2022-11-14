

## Adding reader tests

Reader tests come in two flavors:

 1. Generated tables, which are specified from PySpark code in `dat/generated_tables.py`.
 2. External tables, which are checked in directly at `external-tables`.

Both tables types have their full test data checked into the `out/reader_tests/` folder.

## Writer tests

TBD


## Installation

You can install the dependencies for this project by running `poetry install`.  Run `poetry shell` to activate the virtual environment.    If `poetry shell` does not activate the environment on your machine, you may need to run this command:

```
source "$( poetry env list --full-path | grep Activated | cut -d' ' -f1 )/bin/activate"
```

Run `python -m dat.main` to make sure everything is working properly.  You should see instructions on how to use DAT, including the commands you can run.

## How to build tables

Create the reference tables with `python -m dat.main write-generated-reference-tables`.

This will create reference tables as follows:

```
out/
  tables/
    generated/
      reference_table_1/
        delta/
          _delta_log/
            000.json
            001.json
          parquet_files
        parquet/
          table_content.parquet
        table_metadata.json
      reference_table_2/
        ...
```

Let's take a look at the contents of `reference_table_1`:

* `delta`: This is a Delta Lake table.  It consists of a transaction log (`_delta_log`) and data stored in Parquet files.
* `parquet/table_content.parquet`: Contains all the data that should be in the Delta Lake table when it's read.  This is the expected value.
* `table_metadata.json`: contains the metadata that the `_delta_log` is expected to contain.  This is another expected value.

For each reference table, you should run these checks:

* Read the Delta Lake and make sure it contains the same data as `parquet/table_content.parquet`
* Read the Delta Lake transaction log and make sure it contains the same data as `table_metadata.json`

## Example reader integration tests

Here's an example reader integration test:

```python
import chispa

# check table content
actual_df = spark.read.format("delta").load("out/tables/generated/reference_table_1/delta")
expected_df = spark.read.format("parquet").load("out/tables/generated/reference_table_1/parquet")
chispa.assert_df_equality(actual_df, expected_df)

# check table metadata
delta_log_path = "out/tables/generated/reference_table_1/delta/delta_log"
expected_metadata_path = "out/tables/generated/reference_table_1/table_metadata.json"
assert_metadata_equality(delta_log_path, expected_metadata_path)
```

You could easily write a script that loops over all the reference tables and runs all the reader integration tests.
