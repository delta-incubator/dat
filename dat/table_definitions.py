from typing import List

from dat.model.row_collections import RowCollection
from dat.model.table import ReferenceTable

reference_table_1 = ReferenceTable(
    table_name='reference_table_1',
    table_description='My first table',
    column_names=['letter', 'number', 'a_float'],
    partition_keys=['letter'],
    row_collections=[
        RowCollection(
            write_mode='overwrite',
            data=[
                ('a', 1, 1.1),
                ('b', 2, 2.2),
                ('c', 3, 3.3)
            ]
        ),
        RowCollection(
            write_mode='append',
            data=[
                ('d', 4, 4.4),
                ('e', 5, 5.5)
            ]
        ),
    ]
)


reference_table_2 = ReferenceTable(
    table_name='reference_table_2',
    table_description='My first table',
    column_names=['letter', 'number', 'a_float'],
    partition_keys=['letter'],
    row_collections=[
        RowCollection(
            write_mode='overwrite',
            data=[
                ('a', 1, 1.1),
                ('b', 2, 2.2),
                ('c', 3, 3.3)
            ]
        ),
        RowCollection(
            write_mode='append',
            data=[
                ('a', 4, 4.4),
                ('e', 5, 5.5)
            ]
        ),
    ]
)

_all_tables = [
    reference_table_1,
    reference_table_2
]


def get_tables(filter: str) -> List[ReferenceTable]:
    if filter.lower() == 'all':
        return _all_tables
    names = map(lambda x: x.lower(), filter.split(','))
    results = []
    for table in _all_tables:
        if table.table_name in names:
            results.append(table)
    return results
