from typing import List

from pydantic import BaseModel, validator

from dat.model.row_collections import RowCollection

_wrong_column_name_message = 'Data {data} does not have the correct number of columns {columns}'  # noqa: E501'


class ReferenceTable(BaseModel):
    table_name: str
    table_description: str
    column_names: List[str]
    partition_keys: List[str]
    row_collections: List[RowCollection]

    @validator('row_collections', allow_reuse=True)
    def data_shape_coherent_with_column_names(cls, row_collections, values):
        if 'column_names' in values:
            columns = values['column_names']
            for row_collection in row_collections:
                for record in row_collection.data:
                    if len(record) != len(columns):
                        raise ValueError(
                            _wrong_column_name_message.format(
                                data=record,
                                columns=columns
                            )
                        )
            return row_collections

    @validator('partition_keys', allow_reuse=True)
    def partition_key_must_be_columns(cls, partition_keys, values):
        if 'column_names' in values:
            common_entries = set(partition_keys).intersection(
                values['column_names'])
            if len(common_entries) != len(partition_keys):
                raise ValueError(
                    'Partition keys should all be columns of the table')
            return partition_keys

    @validator('column_names', allow_reuse=True, check_fields=False)
    def columns_not_empty(cls, column_names):
        if not column_names:
            raise ValueError("Columns can't be empty")
        return column_names

    def output_files_path(self, base_path):
        return '{base_path}/{table_name}'.format(
            base_path=base_path,
            table_name=self.table_name
        )
