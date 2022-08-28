import json
import logging
from pathlib import Path
from typing import Dict, List

from dat.model.table import ReferenceTable

_inconsistent_err_msg = 'Inconsistent metadata for external tables. ' + \
    'Tables found: {observed} . Metadata available: {expected}'


def write_table_metadata(path: str, table: ReferenceTable):
    _write_table_metadata(
        Path(path),
        table
    )


def write_all_metadata(folders: List[str], tables: List[ReferenceTable]):
    tables_dict = {table.table_name: table for table in tables}
    paths = [Path(folder_name) for folder_name in folders]
    _validate_folders_match_tables(paths, tables_dict)
    for path in paths:
        table = tables_dict[path.stem]
        _write_table_metadata(path, table)


def _write_table_metadata(path: Path, table: ReferenceTable):
    path.mkdir(parents=True, exist_ok=True)
    metadata_path = path / 'table-metadata.json'
    logging.info(
        'Saving metadata for {table} at {path}'.format(
            table=table.table_name,
            path=metadata_path.as_posix()
        )
    )
    with open(metadata_path.as_posix(), 'w') as outfile:
        json.dump(
            table.dict(),
            fp=outfile,
            indent=2,
        )


def _validate_folders_match_tables(
    paths: List[Path],
    tables: Dict[str, ReferenceTable]
):
    table_folders = [path.stem for path in paths]
    available_metadata = tables.keys()
    if set(table_folders) != available_metadata:
        raise ValueError(
            _inconsistent_err_msg.format(
                observed=table_folders,
                expected=list(available_metadata)
            )
        )
