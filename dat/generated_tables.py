import os
from typing import Callable, List, Optional, Tuple

from dat.models import ExpectedMetadata, TableMetadata
from dat.spark_builder import create_spark_session

spark = create_spark_session()

registered_reference_tables: List[Tuple[TableMetadata, Callable]] = []


def save_expected(
        metadata: TableMetadata, version: Optional[int] = None) -> None:
    '''Save the specified version of a Delta Table as a Parquet file.'''
    query = spark.read.format('delta')
    if version:
        query = query.option('versionAsOf', version)
    df = query.load(metadata.delta_root)

    # Need to ensure directory exists first
    os.makedirs(metadata.expected_root(version))

    df.toPandas().to_parquet(metadata.expected_path(version))

    # TODO: how should we populate this?
    expected_metadata = ExpectedMetadata(
        config={},
        min_reader_version=1,
        min_writer_version=2,
        table_schema_json='{}',
    )

    out_path = metadata.expected_root(version) / 'expected_metadata.json'
    with open(out_path, 'w') as f:
        f.write(expected_metadata.json(indent=2))


def reference_table(name: str, description: str,
                    check_versions: Optional[List[int]] = None):
    metadata = TableMetadata(
        name=name,
        description=description,
        check_versions=check_versions or []
    )

    def wrapper(create_table):
        def inner():
            create_table(metadata)

            with open(metadata.root / 'table_metadata.json', 'w') as f:
                f.write(metadata.json(indent=2, separators=(',', ': ')))

            # Write out latest
            save_expected(metadata)
            # Write expected data for each version requested
            for version in metadata.check_versions:
                save_expected(metadata, version)

        registered_reference_tables.append((metadata, inner))

        return inner

    return wrapper


@reference_table(
    name='basic_append',
    description='A basic table with two append writes.',
    check_versions=[0, 1]
)
def create_basic_append(metadata: TableMetadata):
    columns = ['letter', 'number', 'a_float']
    data = [('a', 1, 1.1), ('b', 2, 2.2), ('c', 3, 3.3)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)

    df.repartition(1).write.format('delta').save(metadata.delta_root)

    data = [('d', 4, 4.4), ('e', 5, 5.5)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)

    df.repartition(1).write.format('delta').mode(
        'append').save(metadata.delta_root)


@reference_table(
    name='basic_partitioned',
    description='A basic partitioned table',
    check_versions=[0, 1],
)
def create_basic_partitioned(metadata: TableMetadata):
    # TODO: include null values in partitioning column
    columns = ['letter', 'number', 'a_float']
    data = [('a', 1, 1.1), ('b', 2, 2.2), ('c', 3, 3.3)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)

    df.repartition(1).write.partitionBy('letter').format(
        'delta').save(metadata.delta_root)

    data = [('a', 4, 4.4), ('e', 5, 5.5)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)

    df.repartition(1).write.partitionBy('letter').format(
        'delta').mode('append').save(metadata.delta_root)


@reference_table(
    name='multi_partitioned',
    description='A table with multiple partitioning columns',
    check_versions=[0, 1, 2],
)
def create_reference_table_3(metadata: TableMetadata):
    columns = ['letter', 'number']
    data = [('a', 1), ('b', 2), ('c', 3)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)

    df.repartition(1).write.format('delta').save(metadata.delta_root)

    data = [('d', 4), ('e', 5)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)

    df.repartition(1).write.format('delta').mode(
        'append').save(metadata.delta_root)

    data = [('x', 66), ('y', 77)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)

    df.repartition(1).write.format('delta').mode(
        'overwrite').save(metadata.delta_root)


@reference_table(
    name='with_schema_change',
    description='Table which has schema change using overwriteSchema=True.',
    check_versions=[1]
)
def with_schema_change(metadata: TableMetadata):
    columns = ['letter', 'number']
    data = [('a', 1), ('b', 2), ('c', 3)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)

    df.repartition(1).write.format('delta').save(metadata.delta_root)

    columns = ['num1', 'num2']
    data2 = [(22, 33), (44, 55), (66, 77)]
    rdd = spark.sparkContext.parallelize(data2)
    df = rdd.toDF(columns)

    df.repartition(1).write.mode('overwrite').option(
        'overwriteSchema', True).format('delta').save(
        metadata.delta_root)
