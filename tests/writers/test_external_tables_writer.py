from pathlib import Path

import pytest
import rootpath

from dat import external_tables
from dat.model.table import ReferenceTable
from dat.writers import external_tables_writer


@pytest.fixture()
def valid_external_table():
    return external_tables.all_tables[0]


@pytest.fixture()
def external_tables_path_abs_path():
    return '{project_root}/external-tables/'.format(
        project_root=rootpath.detect(),
    )


@pytest.fixture()
def valid_external_table_data_path(
    external_tables_path_abs_path,  # noqa: WPS442
    valid_external_table,  # noqa: WPS442
):
    return '{path}/{table_name}'.format(
        path=external_tables_path_abs_path,
        table_name=valid_external_table.table_name,
    )


@pytest.fixture(autouse=True)
def write_first_external_table_to_out(
    tmp_path,
    external_tables_path_abs_path,  # noqa: WPS442
    valid_external_table,  # noqa: WPS442
):
    external_tables_writer.write_external_tables(
        external_tables_data_path=Path(external_tables_path_abs_path),
        tables=[valid_external_table],
        output_path=tmp_path,
    )


@pytest.fixture()
def external_table_output_path(tmp_path, valid_external_table):  # noqa: WPS442
    return tmp_path / valid_external_table.table_name


def test_writing_table_has_written_metadata(
    valid_external_table,  # noqa: WPS442
    external_table_output_path,  # noqa: WPS442
):
    table = ReferenceTable.parse_file(
        (external_table_output_path / 'table-metadata.json').as_posix(),
    )
    assert table == valid_external_table


def test_too_few_tables_fails(tmp_path_factory):
    output = tmp_path_factory.mktemp('temp')
    src = tmp_path_factory.mktemp('src')
    with pytest.raises(ValueError) as exec_info:
        external_tables_writer.write_external_tables(
            external_tables_data_path=src,
            tables=external_tables.all_tables,
            output_path=output,
        )
    assert_inconsistent_metadata(exec_info)  # noqa: WPS441


def test_too_many_tables_fails(tmp_path_factory):
    output = tmp_path_factory.mktemp('temp')
    src = tmp_path_factory.mktemp('src')
    (src / 'unknown_table').mkdir()
    with pytest.raises(ValueError) as exec_info:
        external_tables_writer.write_external_tables(
            external_tables_data_path=src,
            tables=[],
            output_path=output,
        )
    assert_inconsistent_metadata(exec_info)  # noqa: WPS441


def test_missing_tables_fails(tmp_path_factory):
    output = tmp_path_factory.mktemp('temp')
    src = tmp_path_factory.mktemp('src')
    (src / 'unknown_table').mkdir()
    with pytest.raises(ValueError) as exec_info:
        external_tables_writer.write_external_tables(
            external_tables_data_path=src,
            tables=external_tables.all_tables,
            output_path=output,
        )
    assert_inconsistent_metadata(exec_info)  # noqa: WPS441


def assert_inconsistent_metadata(exec_info):
    exec_info.match(
        'Inconsistent metadata for external tables. ',
    )
