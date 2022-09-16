from dat import external_tables
from dat.model.table import ReferenceTable
from dat.writers import metadata_writer


def test_writing_metadata_succeeds(tmp_path):
    first_table = external_tables.all_tables[0]
    table_path = tmp_path / first_table.table_name
    table_path.mkdir()
    metadata_writer.write_table_metadata(
        path=table_path,
        table=first_table,
    )
    table = ReferenceTable.parse_file(
        (table_path / 'table-metadata.json').as_posix(),
    )
    assert table == first_table
