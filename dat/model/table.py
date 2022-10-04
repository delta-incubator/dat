from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field, Json, validator

from dat.model.row_collections import RowCollection

_wrong_column_name_message = 'Data {data} does not have the correct number of columns {columns}'  # noqa: E501'


class ReferenceTable(BaseModel):
    table_name: str
    table_description: str
    column_names: List[str]
    partition_keys: List[str]
    reader_protocol_version: int
    writer_protocol_version: int
    schema_: Optional[Json] = Field(None, alias='schema')  # noqa: WPS120
    features: Optional[List[str]] = []

    @validator('partition_keys', allow_reuse=True)
    def partition_key_must_be_columns(
        cls,  # noqa: N805
        partition_keys,
        values,  # noqa: WPS110
    ):
        columns = values.get('column_names')
        if columns:
            common_entries = set(partition_keys).intersection(
                columns,
            )
            if len(common_entries) != len(partition_keys):
                raise ValueError(
                    'Partition keys should all be columns of the table',
                )
            return partition_keys

    @validator('column_names', allow_reuse=True, check_fields=False)
    def columns_not_empty(cls, column_names):  # noqa: N805
        if not column_names:
            raise ValueError("Columns can't be empty")
        return column_names


class GeneratedReferenceTable(ReferenceTable):
    row_collections: List[RowCollection] = Field(..., exclude=True)

    @validator('row_collections', allow_reuse=True)
    def data_shape_coherent_with_column_names(
        cls,  # noqa: N805
        row_collections,
        values,  # noqa: WPS110
    ):
        columns = values.get('column_names')
        if columns:
            for row_collection in row_collections:
                for record in row_collection.rows:
                    _raise_if_record_has_wrong_col(
                        record,
                        columns,
                    )
            return row_collections

    def output_files_path(self, base_path: Path):
        return '{base_path}/{table_name}'.format(
            base_path=base_path,
            table_name=self.table_name,
        )


def _raise_if_record_has_wrong_col(
    record,
    columns,
):
    if len(record) != len(columns):
        raise ValueError(
            _wrong_column_name_message.format(
                data=record,
                columns=columns,
            ),
        )
