import os
from typing import List, Tuple

from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession

from dat.model.table import ReferenceTable
from dat.model.write_mode import WriteMode


class WritePlan(BaseModel):
    table: ReferenceTable
    entries: List[Tuple[WriteMode, DataFrame]]

    class Config:
        arbitrary_types_allowed = True


class WritePlanBuilder(BaseModel):
    spark: SparkSession

    class Config:
        arbitrary_types_allowed = True

    def build_write_plan(self, table: ReferenceTable) -> WritePlan:
        entries = []
        current_df = None
        for collection in table.row_collections:
            df = self._row_collection_as_df(collection.data, table)
            if current_df:
                current_df = current_df.union(df)
            else:
                current_df = df
            entries.append((collection.write_mode, current_df))
        return WritePlan(
            table=table,
            entries=entries
        )

    def _row_collection_as_df(
        self,
        data: List[Tuple],
        table: ReferenceTable
    ) -> DataFrame:
        return self.spark.createDataFrame(
            data, table.column_names
        )


def _write_delta(write_plan: WritePlan, path: str) -> None:
    for (write_mode, entry) in write_plan.entries:
        entry.write.partitionBy(
            write_plan.table.partition_keys
        ).format(
            'delta'
        ).mode(
            write_mode
        ).save(
            path
        )


def _rewrite_delta_as_parquet(
    spark: SparkSession,
    *,
    source_path: str,
    target_path: str
):
    df = spark.read.format(
        'delta'
    ).load(
        source_path
    )
    os.makedirs(target_path, exist_ok=True)
    df.toPandas().to_parquet(
        '{path}/table_content.parquet'.format(
            path=target_path
        )
    )


def write(spark: SparkSession,
          write_plan: WritePlan,
          base_path: str
          ) -> None:
    table_basepath = write_plan.table.output_files_path(
        base_path
    )
    delta_path = table_basepath + '/delta'
    parquet_path = table_basepath + '/parquet/'
    _write_delta(write_plan, delta_path)
    _rewrite_delta_as_parquet(
        spark,
        source_path=delta_path,
        target_path=parquet_path
    )
