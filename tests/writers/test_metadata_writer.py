import pytest

from dat import external_tables
from dat.model.table import ReferenceTable
from dat.writers import metadata_writer


def test_writing_metadata_succeeds(tmp_path):
    first_table = external_tables.all[0]
    table_path = tmp_path / first_table.table_name
    table_path.mkdir()
    metadata_writer.write_all_metadata(
        [table_path.as_posix()],
        [first_table]
    )
    table = ReferenceTable.parse_file(
        (table_path / 'table-metadata.json').as_posix()
    )
    assert table == first_table


def test_too_few_tables_fails():
    with pytest.raises(ValueError) as exec_info:
        metadata_writer.write_all_metadata(
            [],
            external_tables.all
        )
    assert_inconsistent_metadata(exec_info)


def test_too_many_tables_fails():
    with pytest.raises(ValueError) as exec_info:
        metadata_writer.write_all_metadata(
            ['example'],
            []
        )
    assert_inconsistent_metadata(exec_info)


def test_missing_tables_fails():
    with pytest.raises(ValueError) as exec_info:
        metadata_writer.write_all_metadata(
            ['example'],
            external_tables.all
        )
    assert_inconsistent_metadata(exec_info)


def assert_inconsistent_metadata(exec_info):
    exec_info.match(
            'Inconsistent metadata for external tables. '
    )
