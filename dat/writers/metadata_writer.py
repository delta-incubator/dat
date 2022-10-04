import json
import logging
from pathlib import Path

from dat.model.table import ReferenceTable


def write_table_metadata(
    *,
    path: Path,
    table: ReferenceTable,
):
    path.mkdir(parents=True, exist_ok=True)
    metadata_path = path / 'table-metadata.json'
    logging.info(
        'Saving metadata for {table} at {path}'.format(
            table=table.table_name,
            path=metadata_path.as_posix(),
        ),
    )
    with open(metadata_path.as_posix(), 'w') as outfile:
        json.dump(
            table.dict(),
            fp=outfile,
            indent=2,
        )
