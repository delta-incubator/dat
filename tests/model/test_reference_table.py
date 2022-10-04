import pytest
from pydantic import ValidationError

from dat.model.row_collections import RowCollection
from dat.model.table import GeneratedReferenceTable


def test_data_has_more_columns_than_table_fails():
    with pytest.raises(ValidationError) as exec_info:
        GeneratedReferenceTable(
            table_name='failing_table',
            table_description='',
            column_names=['a', 'b'],
            partition_keys=[],
            writer_protocol_version=2,
            reader_protocol_version=2,
            row_collections=[
                RowCollection(
                    write_mode='append',
                    rows=[
                        (1, 2, 3),
                    ],
                ),
            ],
        )
    exec_info.match(  # noqa: WPS441
        ".*Data \\(1, 2, 3\\) does not have the correct number of columns \\['a', 'b'\\].*",  # noqa: W605 E501 WPS342
    )


def test_data_has_fewer_columns_than_table_fails():
    with pytest.raises(ValidationError) as exec_info:
        GeneratedReferenceTable(
            table_name='failing_table',
            table_description='',
            column_names=['a', 'b'],
            partition_keys=[],
            writer_protocol_version=2,
            reader_protocol_version=2,
            row_collections=[
                RowCollection(
                    write_mode='append',
                    rows=[
                        (1,),
                    ],
                ),
            ],
        )
    exec_info.match(  # noqa: WPS441
        ".*Data \\(1,\\) does not have the correct number of columns \\['a', 'b'\\].*",  # noqa: W605 E501 WPS342
    )


def test_invalid_partition_key_fails():
    with pytest.raises(ValidationError) as exec_info:
        GeneratedReferenceTable(
            table_name='failing_table',
            table_description='',
            column_names=['a', 'b'],
            partition_keys=['c'],
            writer_protocol_version=2,
            reader_protocol_version=2,
            data=[('1', '2')],
        )
    exec_info.match('Partition keys should all be columns of the table')  # noqa: WPS441 E501


def test_table_with_no_columns():
    with pytest.raises(ValidationError) as exec_info:
        GeneratedReferenceTable(
            table_name='failing_table',
            table_description='',
            column_names=[],
            partition_keys=[],
            writer_protocol_version=2,
            reader_protocol_version=2,
            row_collections=[],
        )
    exec_info.match(" Columns can't be empty ")  # noqa: WPS441
