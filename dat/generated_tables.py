from typing import Sequence

from dat.model.row_collections import RowCollection
from dat.model.table import GeneratedReferenceTable, ReferenceTable

reference_table_1 = GeneratedReferenceTable(
    table_name='reference_table_1',
    table_description='My first table',
    column_names=['letter', 'number', 'a_float'],
    partition_keys=['letter'],
    reader_protocol_version=2,
    writer_protocol_version=2,
    row_collections=[
        RowCollection(
            write_mode='overwrite',
            rows=[
                ('a', 1, 1.1),
                ('b', 2, 2.2),
                ('c', 3, 3.3),
            ],
        ),
        RowCollection(
            write_mode='append',
            rows=[
                ('d', 4, 4.4),
                ('e', 5, 5.5),
            ],
        ),
    ],
)


reference_table_2 = GeneratedReferenceTable(
    table_name='reference_table_2',
    table_description='My first table',
    column_names=['letter', 'number', 'a_float'],
    partition_keys=['letter'],
    reader_protocol_version=2,
    writer_protocol_version=2,
    row_collections=[
        RowCollection(
            write_mode='overwrite',
            rows=[
                ('a', 1, 1.1),
                ('b', 2, 2.2),
                ('c', 3, 3.3),
            ],
        ),
        RowCollection(
            write_mode='append',
            rows=[
                ('a', 4, 4.4),
                ('e', 5, 5.5),
            ],
        ),
    ],
)

_all_tables = [
    reference_table_1,
    reference_table_2,
]


def get_tables(included_tablenames: str) -> Sequence[ReferenceTable]:
    if included_tablenames.lower() == 'all':
        return _all_tables
    names = [
        tablename.lower()
        for tablename in included_tablenames.split(',')
    ]
    included_tables = []
    for table in _all_tables:
        if table.table_name in names:
            included_tables.append(table)
    return included_tables
