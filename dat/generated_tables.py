import os
from datetime import date
from typing import Callable, List, Tuple

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from dat.models import TableVersionMetadata, TestCaseInfo
from dat.spark_builder import get_spark_session

registered_reference_tables: List[Tuple[TestCaseInfo, Callable]] = []


def get_version_metadata(case: TestCaseInfo) -> TableVersionMetadata:
    spark = get_spark_session()
    table = DeltaTable.forPath(spark, case.delta_root)
    detail = table.detail().collect()[0]

    return TableVersionMetadata(
        version=table.history(1).collect()[0].version,
        properties=detail['properties'],
        min_reader_version=detail['minReaderVersion'],
        min_writer_version=detail['minWriterVersion'],
    )


def save_expected(case: TestCaseInfo, as_latest=False) -> None:
    '''Save the specified version of a Delta Table as a Parquet file.'''
    spark = get_spark_session()
    df = spark.read.format('delta').load(case.delta_root)

    version_metadata = get_version_metadata(case)
    version = None if as_latest else version_metadata.version

    # Need to ensure directory exists first
    os.makedirs(case.expected_root(version))

    df.toPandas().to_parquet(case.expected_path(version))

    out_path = case.expected_root(version) / 'table_version_metadata.json'
    with open(out_path, 'w') as f:
        f.write(version_metadata.json(indent=2))


def reference_table(name: str, description: str):
    case = TestCaseInfo(
        name=name,
        description=description
    )

    def wrapper(create_table):
        def inner():
            spark = get_spark_session()
            create_table(case, spark)

            with open(case.root / 'test_case_info.json', 'w') as f:
                f.write(case.json(indent=2, separators=(',', ': ')))

            # Write out latest
            save_expected(case, as_latest=True)

        registered_reference_tables.append((case, inner))

        return inner

    return wrapper


@reference_table(
    name='basic_append',
    description='A basic table with two append writes.'
)
def create_basic_append(case: TestCaseInfo, spark: SparkSession):
    columns = ['letter', 'number', 'a_float']
    data = [('a', 1, 1.1), ('b', 2, 2.2), ('c', 3, 3.3)]
    df = spark.createDataFrame(data, schema=columns)

    df.repartition(1).write.format('delta').save(case.delta_root)
    save_expected(case)

    data = [('d', 4, 4.4), ('e', 5, 5.5)]
    df = spark.createDataFrame(data, schema=columns)

    df.repartition(1).write.format('delta').mode(
        'append').save(case.delta_root)
    save_expected(case)


@reference_table(
    name='basic_partitioned',
    description='A basic partitioned table',
)
def create_basic_partitioned(case: TestCaseInfo, spark: SparkSession):
    columns = ['letter', 'number', 'a_float']
    data = [('a', 1, 1.1), ('b', 2, 2.2), ('c', 3, 3.3)]
    df = spark.createDataFrame(data, schema=columns)

    df.repartition(1).write.partitionBy('letter').format(
        'delta').save(case.delta_root)
    save_expected(case)

    data2 = [('a', 4, 4.4), ('e', 5, 5.5), (None, 6, 6.6)]
    df = spark.createDataFrame(data2, schema=columns)

    df.repartition(1).write.partitionBy('letter').format(
        'delta').mode('append').save(case.delta_root)
    save_expected(case)


@reference_table(
    name='multi_partitioned',
    description=('A table with multiple partitioning columns. Partition '
                 'values include nulls and escape characters.'),
)
def create_multi_partitioned(case: TestCaseInfo, spark: SparkSession):
    columns = ['letter', 'date', 'data', 'number']
    partition_columns = ['letter', 'date', 'data']
    data = [
        ('a', date(1970, 1, 1), b'hello', 1),
        ('b', date(1970, 1, 1), b'world', 2),
        ('b', date(1970, 1, 2), b'world', 3)
    ]
    df = spark.createDataFrame(data, schema=columns)
    # rdd = spark.sparkContext.parallelize(data)
    # df = rdd.toDF(columns)
    schema = df.schema

    df.repartition(1).write.format('delta').partitionBy(
        *partition_columns).save(case.delta_root)
    save_expected(case)

    # Introduce null values in partition columns
    data2 = [
        ('a', None, b'x', 4),
        (None, None, None, 5)
    ]
    df = spark.createDataFrame(data2, schema=schema)

    df.repartition(1).write.format('delta').mode(
        'append').save(case.delta_root)
    save_expected(case)

    # Introduce escape characters
    data3 = [
        ('/%20%f', date(1970, 1, 1), b'hello', 6),
        ('b', date(1970, 1, 1), '😈'.encode(), 7)
    ]
    df = spark.createDataFrame(data3, schema=schema)

    df.repartition(1).write.format('delta').mode(
        'overwrite').save(case.delta_root)
    save_expected(case)


@reference_table(
    name='with_schema_change',
    description='Table which has schema change using overwriteSchema=True.',
)
def with_schema_change(case: TestCaseInfo, spark: SparkSession):
    columns = ['letter', 'number']
    data = [('a', 1), ('b', 2), ('c', 3)]
    df = spark.createDataFrame(data, schema=columns)

    df.repartition(1).write.format('delta').save(case.delta_root)

    columns = ['num1', 'num2']
    data2 = [(22, 33), (44, 55), (66, 77)]
    df = spark.createDataFrame(data2, schema=columns)

    df.repartition(1).write.mode('overwrite').option(
        'overwriteSchema', True).format('delta').save(
        case.delta_root)
    save_expected(case)
