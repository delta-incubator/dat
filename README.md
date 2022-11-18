# Delta Acceptance Testing (DAT)

The DAT project provides test cases to verify different implementations of Delta Lake all behave consistently. The expected behavior is described in the [Delta Lake Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md).

The test cases in this repo are represented using a standard file structure, so they don't require any particular dependency or programming language. Delta implementations can clone (or submodule) this repo and run their integration tests against the tables located in the `out` directory.

## Testing Readers

All reader test cases are stored in the directory `out/reader_tests/generated`. They follow the directory structure:

```
|-- {table_name}
  |-- test_case_info.json
  |-- delta
  | |-- _delta_log
  |   |-- ...
  | |-- part-0000-dsfsadf-adsfsdaf-asdf.snappy.parquet
  | |-- ...
  |-- expected
    |-- latest
      |-- table_version_metadata.json
      |-- table_content.parquet
    |-- v1
      |-- table_version_metadata.json
      |-- table_content.parquet
```

Each test case is a folder, named for it's test. It contains:

 * `test_case_info.json`: document that provides the names and human-friendly description of the test.
 * `delta`: the root directory of the Delta Lake table.
 * `expected`: a folder containing expected results, potentially for multiple versions of the table. At a minimum, there is a folder called `latest` containing the expected data for the current version of the table. There may be other folders such as `v1`, `v2`, and so on for other versions for testing time travel. There are two types of files in each version folder:
   * Parquet files, that contain the expected data.
   * A JSON file, `table_version_metadata.json` which contains the metadata about that version of the table. For example, it contains the protocol versions `min_reader_version` and `min_writer_version`.

To test a reader, readers should first identify the test cases:

 1. List the `out/reader_tests/generated/` directory to identify the root of each Delta table.
 2. List each subdirectory's `expected` directory; each of these folders represents a test case.

Then for each test case:

 1. Load the corresponding version of the Delta table
 2. Verify the metadata read from the delta table matches that in the expected_metadata.json
 3. Attempt to read the Delta table's data:
   a. If the Delta table uses a version unsupported by the reader connector (as determined from `table_version_metadata.json`), verify an appropriate error is returned.
   b. If the Delta table is supported by the reader connector, assert that the read data is equal to the data read from `table_content.parquet`.

For an example implementation of this, see the example PySpark tests in `tests/pyspark_delta/`.

## Testing Writers

TBD.

## Contributing

See [contributing.md](./contributing.md).
