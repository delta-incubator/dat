import logging
import os
import shutil
from pathlib import Path
from typing import Optional

import click

from dat import generated_tables
from dat.models import TableVersionMetadata, TestCaseInfo

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
@click.option('--table-name')
def write_generated_reference_tables(table_name: Optional[str]):
    if table_name:
        for metadata, create_table in generated_tables.registered_reference_tables:
            if metadata.name == table_name:
                logging.info("Writing table '%s'", metadata.name)
                out_base = Path('out/reader_tests/generated') / table_name
                shutil.rmtree(out_base, ignore_errors=True)

                create_table()
                break
        else:
            raise ValueError(
                f"Could not find generated table named '{table_name}'")
    else:
        out_base = Path('out/reader_tests/generated')
        shutil.rmtree(out_base, ignore_errors=True)

        for metadata, create_table in generated_tables.registered_reference_tables:
            logging.info("Writing table '%s'", metadata.name)
            create_table()


@click.command()
def write_model_schemas():
    out_base = Path('out/schemas')
    os.makedirs(out_base, exist_ok=True)

    with open(out_base / 'TestCaseInfo.json', 'w') as f:
        f.write(TestCaseInfo.schema_json(indent=2))

    with open(out_base / 'TableVersionMetadata.json', 'w') as f:
        f.write(TableVersionMetadata.schema_json(indent=2))


cli.add_command(write_generated_reference_tables)
cli.add_command(write_model_schemas)

if __name__ == '__main__':
    cli()
