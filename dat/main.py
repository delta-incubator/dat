import logging
import os
from pathlib import Path

import click

from dat import generated_tables
from dat.models import ExpectedMetadata, TableMetadata

logging.basicConfig(
    level=logging.INFO,
)


@click.group()
def cli():
    """
    DAT (Delta Acceptance Testing) CLI helper.

    This CLI tool helps performing mundane tasks related to managing
    reference tables for delta acceptance testing, including:

    - generating tables from python code

    - generating json schemas of the metadata for code generation
        in other programming languages


    """
    pass  # noqa: WPS420


@click.command()
def write_generated_reference_tables():
    for metadata, create_table in generated_tables.registered_reference_tables:
        logging.info("Writing table '%s'", metadata.name)
        create_table()


@click.command()
def write_model_schemas():
    out_base = Path('out/schemas')
    os.makedirs(out_base)

    with open(out_base / 'table_metadata.json', 'w') as f:
        f.write(TableMetadata.schema_json(indent=2))

    with open(out_base / 'expected_metadata.json', 'w') as f:
        f.write(ExpectedMetadata.schema_json(indent=2))


cli.add_command(write_generated_reference_tables)
cli.add_command(write_model_schemas)

if __name__ == '__main__':
    cli()
