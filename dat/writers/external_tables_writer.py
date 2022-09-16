import logging
import os
import shutil
from pathlib import Path
from typing import Dict, List

from dat.model.table import ReferenceTable
from dat.writers import metadata_writer

_inconsistent_err_msg = (
    'Inconsistent metadata for external tables. ' +
    'Delta Log and Data found: {observed} . Metadata available: {expected}'
)


def write_external_tables(
    *,
    external_tables_data_path: Path,
    tables: List[ReferenceTable],
    output_path: Path,
):
    tables_dict = {table.table_name: table for table in tables}
    table_source_folders = _traverse_external_tables_path(
        external_tables_data_path,
    )
    tables_sources_path = [
        Path(folder_name)
        for folder_name in table_source_folders
    ]
    _validate_folders_match_tables(tables_sources_path, tables_dict)
    for table_source_path in tables_sources_path:
        _copy_data_and_write_metadata(
            table_source_path=table_source_path,
            table=tables_dict[table_source_path.stem],
            output_path=output_path,
        )


def _copy_data_and_write_metadata(
    *,
    table_source_path: Path,
    table: ReferenceTable,
    output_path: Path,
):
    table_output_path = (
        output_path / table.table_name
    )
    metadata_writer.write_table_metadata(
        path=table_output_path,
        table=table,
    )
    _copy_table_data(
        source_path=table_source_path,
        output_path=table_output_path,
    )


def _traverse_external_tables_path(external_tables_path: Path):
    subfolders = os.listdir(external_tables_path.as_posix())
    return [
        os.path.join(
            external_tables_path,
            subfolder,
        )
        for subfolder in subfolders
    ]


def _copy_table_data(
    *,
    source_path: Path,
    output_path: Path,
):
    logging.info(
        'Recursively copying {source_path} to {output_path}'.format(
            source_path=source_path,
            output_path=output_path,
        ),
    )
    shutil.copytree(
        src=source_path,
        dst=output_path,
        dirs_exist_ok=True,
    )


def _validate_folders_match_tables(
    paths: List[Path],
    tables: Dict[str, ReferenceTable],
):
    table_folders = [path.stem for path in paths]
    available_metadata = tables.keys()
    if set(table_folders) != available_metadata:
        raise ValueError(
            _inconsistent_err_msg.format(
                observed=table_folders,
                expected=list(available_metadata),
            ),
        )
