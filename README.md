# Delta Acceptance Testing (DAT)

The DAT project makes it easy to generate Delta Lake reference tables and run integration tests to check that different implementations of the Delta Lake PROTOCOL behave as expected.

The test cases in this repo are represented using a standard file structure, so they don't require any particular dependency or programming language. Delta implementations can clone (or submodule) this repo and run their integration tests against the tables located in the `out` directory.

## Testing Readers

All reader test cases are stored in the directory `out/reader_tests`. They follow the directory structure:

```
|-- {table_name}
  |-- case_metadata.json
  |-- delta
  | |-- _delta_log
  |   |-- ...
  | |-- part-0000-dsfsadf-adsfsdaf-asdf.snappy.parquet
  | |-- ...
  |-- expected
    |-- latest
      |-- expected_metadata.json
      |-- table_content.parquet
    |-- v1
      |-- expected_metadata.json
      |-- table_content.parquet
```

Each test case is a folder, named for it's test. It contains:

 * `case_metadata.json`: document that provides the names and human-friendly description of the test.
 * `delta`: the root directory of the Delta Lake table.
 * `expected`: a folder containing expected results, potentially for multiple versions of the table. At a minimum, there is a folder called `latest` containing the expected data for the current version of the table. There may be other folders such as `v1`, `v2`, and so on for other versions for testing time travel. 

To test a reader, readers should first identify the test cases:

 1. List the `out/reader_tests` directory to identify the root of each delta table.
 2. List each subdirectories `expected` directory; each of these folders represents a test case.

Then for each test case:

 1. Load the corresponding version of the delta table
 2. Verify the metadata read from the delta table matches that in the expected_metadata.json
 3. Attempt to read the delta tables data:
   a. If the delta table uses an unsupported version (as determined from expected_metadata.json), verify an appropriate error is returned.
   b. If the delta table is supported, assert that the read data is equal to the data read from `table_content.parquet`.

For an example implementation of this, see the pyspark tests in `tests/pyspark_delta/`.

## Testing Writers

TBD.

## Contributing

See [contributing.md](./contributing.md).
