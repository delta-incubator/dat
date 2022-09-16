import logging
import os
from pathlib import Path
from typing import List, Tuple

from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession

from dat.model.table import GeneratedReferenceTable
from dat.model.write_mode import WriteMode
from dat.writers import metadata_writer


class WritePlan(BaseModel):
    table: GeneratedReferenceTable
    entries: List[Tuple[WriteMode, DataFrame]]

    class Config(object):  # noqa: WPS431
        arbitrary_types_allowed = True


class WritePlanBuilder(BaseModel):
    spark: SparkSession

    class Config(object):  # noqa: WPS431
        arbitrary_types_allowed = True

    def build_write_plan(self, table: GeneratedReferenceTable) -> WritePlan:
        entries = []
        current_df = None
        for collection in table.row_collections:
            df = self._row_collection_as_df(collection.rows, table)
            if current_df:
                current_df = current_df.union(df)
            else:
                current_df = df
            entries.append((collection.write_mode, current_df))
        return WritePlan(
            table=table,
            entries=entries,
        )

    def _row_collection_as_df(
        self,
        rows: List[Tuple],
        table: GeneratedReferenceTable,
    ) -> DataFrame:
        return self.spark.createDataFrame(
            rows,
            table.column_names,
        )


def write(
    spark: SparkSession,
    write_plan: WritePlan,
    base_path: Path,
) -> None:
    table_basepath = write_plan.table.output_files_path(
        base_path,
    )
    delta_path = table_basepath + '/delta'
    parquet_path = table_basepath + '/parquet/'
    _write_delta(write_plan, delta_path)
    _rewrite_delta_as_parquet(
        spark,
        source_path=delta_path,
        target_path=parquet_path,
    )
    metadata_writer.write_table_metadata(
        path=Path(table_basepath),
        table=write_plan.table,
    )


def write_generated_tables(
    spark: SparkSession,
    output_path: Path,
    tables: List[GeneratedReferenceTable],
):
    write_plan_builder = WritePlanBuilder(
        spark=spark,
    )
    write_plans = [
        write_plan_builder.build_write_plan(table)
        for table in tables
    ]
    for write_plan in write_plans:
        logging.info(
            'Writing {table_name}'.format(
                table_name=write_plan.table.table_name,
            ),
        )
        os.makedirs(output_path, exist_ok=True)
        write(
            spark,
            write_plan,
            output_path,
        )


def _write_delta(write_plan: WritePlan, path: str) -> None:
    for (write_mode, entry) in write_plan.entries:
        entry.write.partitionBy(
            write_plan.table.partition_keys,
        ).format(
            'delta',
        ).mode(
            write_mode,
        ).save(
            path,
        )


def _rewrite_delta_as_parquet(
    spark: SparkSession,
    *,
    source_path: str,
    target_path: str,
):
    df = spark.read.format(
        'delta',
    ).load(
        source_path,
    )
    os.makedirs(target_path, exist_ok=True)
    df.toPandas().to_parquet(
        '{path}/table_content.parquet'.format(
            path=target_path,
        ),
    )
