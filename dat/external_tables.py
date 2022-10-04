from dat.model.table import ReferenceTable

example_table = ReferenceTable(
    table_name='my_external_table',
    table_description='My first table',
    column_names=['letter', 'number', 'a_float'],
    partition_keys=['letter'],
    reader_protocol_version=2,
    writer_protocol_version=2,
)

all_tables = [
    example_table,
]
