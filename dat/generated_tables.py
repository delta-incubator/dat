import os
import random
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Callable, List, Tuple

import pyspark.sql
import pyspark.sql.types as types
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
    name='multi_partitioned_2',
    description=('Multiple levels of partitioning, with boolean, timestamp, and '
                 'decimal partition columns')
)
def create_multi_partitioned_2(case: TestCaseInfo, spark: SparkSession):
    columns = ['bool', 'time', 'amount', 'int']
    partition_columns = ['bool', 'time', 'amount']
    data = [
        (True, datetime(1970, 1, 1), Decimal('200.00'), 1),
        (True, datetime(1970, 1, 1, 12, 30), Decimal('200.00'), 2),
        (False, datetime(1970, 1, 2, 8, 45), Decimal('12.00'), 3)
    ]
    df = spark.createDataFrame(data, schema=columns)
    df.repartition(1).write.format('delta').partitionBy(
        *partition_columns).save(case.delta_root)


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


@reference_table(
    name='all_primitive_types',
    description='Table containing all non-nested types',
)
def create_all_primitive_types(case: TestCaseInfo, spark: SparkSession):
    schema = types.StructType([
        types.StructField('utf8', types.StringType()),
        types.StructField('int64', types.LongType()),
        types.StructField('int32', types.IntegerType()),
        types.StructField('int16', types.ShortType()),
        types.StructField('int8', types.ByteType()),
        types.StructField('float32', types.FloatType()),
        types.StructField('float64', types.DoubleType()),
        types.StructField('bool', types.BooleanType()),
        types.StructField('binary', types.BinaryType()),
        types.StructField('decimal', types.DecimalType(5, 3)),
        types.StructField('date32', types.DateType()),
        types.StructField('timestamp', types.TimestampType()),
    ])

    df = spark.createDataFrame([
        (
            str(i),
            i,
            i,
            i,
            i,
            float(i),
            float(i),
            i % 2 == 0,
            bytes(i),
            Decimal('10.000') + i,
            date(1970, 1, 1) + timedelta(days=i),
            datetime(1970, 1, 1) + timedelta(hours=i)
        )
        for i in range(5)
    ], schema=schema)

    df.repartition(1).write.format('delta').save(case.delta_root)


@reference_table(
    name='nested_types',
    description='Table containing various nested types',
)
def create_nested_types(case: TestCaseInfo, spark: SparkSession):
    schema = types.StructType([types.StructField(
        'struct', types.StructType(
            [types.StructField(
                'float64', types.DoubleType()),
             types.StructField(
                'bool', types.BooleanType()), ])),
        types.StructField(
        'array', types.ArrayType(
            types.ShortType())),
        types.StructField(
        'map', types.MapType(
            types.StringType(),
            types.IntegerType())), ])

    df = spark.createDataFrame([
        (
            {'float64': float(i), 'bool': i % 2 == 0},
            list(range(i + 1)),
            {str(i): i for i in range(i)}
        )
        for i in range(5)
    ], schema=schema)

    df.repartition(1).write.format('delta').save(case.delta_root)


def get_sample_data(
        spark: SparkSession, seed: int = 42, nrows: int = 5) -> pyspark.sql.DataFrame:
    # Use seed to get consistent data between runs, for reproducibility
    random.seed(seed)
    return spark.createDataFrame([
        (
            random.choice(['a', 'b', 'c', None]),
            random.randint(0, 1000),
            date(random.randint(1970, 2020), random.randint(1, 12), 1)
        )
        for i in range(nrows)
    ], schema=['letter', 'int', 'date'])


@reference_table(
    name='with_checkpoint',
    description='Table with a checkpoint',
)
def create_with_checkpoint(case: TestCaseInfo, spark: SparkSession):
    df = get_sample_data(spark)

    (DeltaTable.create(spark)
     .location(str(Path(case.delta_root).absolute()))
     .addColumns(df.schema)
     .property('delta.checkpointInterval', '2')
     .execute())

    for i in range(3):
        df = get_sample_data(spark, seed=i, nrows=5)
        df.repartition(1).write.format('delta').mode(
            'overwrite').save(case.delta_root)

    assert any(path.suffixes == ['.checkpoint', '.parquet']
               for path in (Path(case.delta_root) / '_delta_log').iterdir())


def remove_log_file(delta_root: str, version: int):
    os.remove(os.path.join(delta_root, '_delta_log', f'{version:0>20}.json'))


@reference_table(
    name='no_replay',
    description='Table with a checkpoint and prior commits cleaned up',
)
def create_no_replay(case: TestCaseInfo, spark: SparkSession):
    spark.conf.set(
        'spark.databricks.delta.retentionDurationCheck.enabled', 'false')

    df = get_sample_data(spark)

    table = (DeltaTable.create(spark)
             .location(str(Path(case.delta_root).absolute()))
             .addColumns(df.schema)
             .property('delta.checkpointInterval', '2')
             .execute())

    for i in range(3):
        df = get_sample_data(spark, seed=i, nrows=5)
        df.repartition(1).write.format('delta').mode(
            'overwrite').save(case.delta_root)

    table.vacuum(retentionHours=0)

    remove_log_file(case.delta_root, version=0)
    remove_log_file(case.delta_root, version=1)

    files_in_log = list((Path(case.delta_root) / '_delta_log').iterdir())
    assert any(path.suffixes == ['.checkpoint', '.parquet']
               for path in files_in_log)
    assert not any(path.name == f'{0:0>20}.json' for path in files_in_log)


@reference_table(
    name='stats_as_struct',
    description='Table with stats only written as struct (not JSON) with Checkpoint',
)
def create_stats_as_struct(case: TestCaseInfo, spark: SparkSession):
    df = get_sample_data(spark)
    (DeltaTable.create(spark)
        .location(str(Path(case.delta_root).absolute()))
        .addColumns(df.schema)
        .property('delta.checkpointInterval', '2')
        .property('delta.checkpoint.writeStatsAsStruct', 'true')
        .property('delta.checkpoint.writeStatsAsJson', 'false')
        .execute())

    for i in range(3):
        df = get_sample_data(spark, seed=i, nrows=5)
        df.repartition(1).write.format('delta').mode(
            'overwrite').save(case.delta_root)


@reference_table(
    name='no_stats',
    description='Table with no stats',
)
def create_no_stats(case: TestCaseInfo, spark: SparkSession):
    df = get_sample_data(spark)
    (DeltaTable.create(spark)
        .location(str(Path(case.delta_root).absolute()))
        .addColumns(df.schema)
        .property('delta.checkpointInterval', '2')
        .property('delta.checkpoint.writeStatsAsStruct', 'false')
        .property('delta.checkpoint.writeStatsAsJson', 'false')
        .property('delta.dataSkippingNumIndexedCols', '0')
        .execute())

    for i in range(3):
        df = get_sample_data(spark, seed=i, nrows=5)
        df.repartition(1).write.format('delta').mode(
            'overwrite').save(case.delta_root)
