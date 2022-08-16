# dat: Delta Acceptance Testing

Warning: this project is incubating 🚧 🛠.  Any changes are possible in the future, including full deletion of this repo.

This repo contains Delta Lake reference tables.  It specifies functionality that the [Delta connectors](https://github.com/delta-io/connectors) should be able to implement.  The Delta Lake connectors will use these reference tables in integration tests to demonstrate they can perform the requisite read / write operations on Delta Lakes.  This isn't only for that specific Delta connectors repo however.  This is for any connector for Delta Lake, including Delta Lake on Spark.

These tests will ensure that the various Delta connectors are interoperable and help deliver a wonderful user experience to end users.

## Reference tables

See the `reference_tables` directory for the tables you can access in this project.  Here's the layout of the reference tables directory:

```
reference_tables
    reference_table1
        table
            _delta_log
                N.checkpoint
                N+1.json
            uuid1.parquet
            uuid2.parquet
        table_metadata.json
        table_content.parquet
    reference_table2
        ...
    reference_table3
        ...
```

Here's what each reference table contains:

* `table`: A Delta Lake
* `table_metadata.json`: metadata about the Delta Lake stored in a JSON file
* `table_content.parquet`: the "final state" data that should be contained in the Delta Lake

## Develop

This section is only relevant if you want to generate reference tables yourself.  Most users of this repo will just use the reference tables that are pre-generated and checked into this repo.  They don't need to generate the reference tables themselves.

### Setup environment

Create the conda environment on your machine by running `conda env create -f envs/pyspark-322-delta-200.yml`.

Activate the conda environment with `conda activate pyspark-322-delta-200`.

### Create reference tables

Create the reference tables with `python -m dat.create_reference_tables`.

The reference tables are already checked into this repo, so you don't need to create them yourself.
