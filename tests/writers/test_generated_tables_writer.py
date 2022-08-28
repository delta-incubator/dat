import glob
import os
from typing import List, Tuple

from pyspark.sql import DataFrame, SparkSession

from dat import generated_tables
from dat.model.row_collections import RowCollection
from dat.model.table import ReferenceTable
from dat.model.write_mode import WriteMode
from dat.writers import generated_tables_writer
from dat.writers.generated_tables_writer import WritePlan


def test_plan_correct(spark_session, generated_reference_table_1_write_plan):
    assert_plan_steps_match_table(
        spark_session,
        generated_reference_table_1_write_plan,
        generated_tables.reference_table_1
    )


def test_table_metadata_is_written_correctly(
    spark_session,
    generated_reference_table_1_write_plan,
    tmp_path
):
    generated_tables_writer.write(
        spark_session,
        generated_reference_table_1_write_plan,
        tmp_path.as_posix()
    )
    assert_table_metadata_is_correct(
        tmp_path,
        generated_reference_table_1_write_plan.table
    )


def test_table_data_is_written_correctly(
    spark_session,
    generated_reference_table_1_write_plan,
    tmp_path
):
    generated_tables_writer.write(
        spark_session,
        generated_reference_table_1_write_plan,
        tmp_path.as_posix()
    )
    assert_parquet_file_exists_in_right_location(
        tmp_path,
        generated_reference_table_1_write_plan.table
    )
    assert_delta_log_exists_in_right_location(
        tmp_path,
        generated_reference_table_1_write_plan.table
    )
    assert_delta_table_is_partitioned(
        tmp_path,
        generated_reference_table_1_write_plan.table
    )


def assert_parquet_file_exists_in_right_location(base_path, table):
    parquet_location = '{table_path}/parquet/table_content.parquet'.format(
        table_path=table.output_files_path(base_path)
    )
    assert os.path.exists(parquet_location)


def assert_delta_log_exists_in_right_location(base_path, table):
    delta_log_location = '{table_path}/delta/_delta_log'.format(
        table_path=table.output_files_path(base_path)
    )
    assert len(os.listdir(delta_log_location)) > 0


def assert_delta_table_is_partitioned(base_path, table):
    partitions_path = '{table_path}/delta/letter*'.format(
        table_path=table.output_files_path(base_path)
    )
    partitions = glob.glob(partitions_path)
    assert len(partitions) == 5


def assert_plan_steps_match_table(
    spark: SparkSession,
    plan: WritePlan,
    table: ReferenceTable
):
    assert plan.table == table
    assert len(plan.entries) == len(table.row_collections)
    assert_dataframes_match_row_collections(
        spark,
        table.column_names,
        plan.entries,
        table.row_collections
    )


def assert_dataframes_match_row_collections(
    spark: SparkSession,
    column_names: List[str],
    plan_entries: List[Tuple[WriteMode, DataFrame]],
    row_collections: List[RowCollection]
):
    current_df = None
    for plan_entry, row_collection in zip(plan_entries, row_collections):
        df = spark.createDataFrame(row_collection.data, column_names)
        if current_df:
            current_df = current_df.union(df)
        else:
            current_df = df
        assert plan_entry[1].collect() == current_df.collect()
        assert plan_entry[0] == row_collection.write_mode


def assert_table_metadata_is_correct(base_path, table):
    observed_table = ReferenceTable.parse_file(
        (base_path / table.table_name / 'table-metadata.json').as_posix()
    )
    assert table == observed_table
