import json
import logging
import os
import shutil
from pathlib import Path
from typing import Annotated

import typer

from dat import generated_tables
from dat.models import TableVersionMetadata, TestCaseInfo

logging.basicConfig(
    level=logging.INFO,
)


app = typer.Typer()


@app.callback()
def main():
    """
    DAT (Delta Acceptance Testing) CLI helper.

    This CLI tool helps to perform mundane tasks related to managing
    reference tables for delta acceptance testing, including:

    - generating tables from python code
    - generating json schemas of the metadata for code generation
      in other programming languages


    """
    pass


@app.command()
def generate_tables(
    table_name: Annotated[
        str | None,
        typer.Option(
            help="Name of the table to write. Will write only the specified table."
        ),
    ] = None,
):
    """Generate DAT reference Tables."""

    if table_name:
        for metadata, create_table in generated_tables.registered_reference_tables:
            if metadata.name == table_name:
                logging.info("Writing table '%s'", metadata.name)
                out_base = Path("out/reader_tests/generated") / table_name
                shutil.rmtree(out_base, ignore_errors=True)

                create_table()
                break
        else:
            raise ValueError(f"Could not find generated table named '{table_name}'")
    else:
        out_base = Path("out/reader_tests/generated")
        shutil.rmtree(out_base, ignore_errors=True)

        for metadata, create_table in generated_tables.registered_reference_tables:
            logging.info("Writing table '%s'", metadata.name)
            create_table()


@app.command()
def generate_schemas():
    """Generate JSON schemas."""

    out_base = Path("out/schemas")
    os.makedirs(out_base, exist_ok=True)

    with open(out_base / "TestCaseInfo.json", "w") as f:
        f.write(json.dumps(TestCaseInfo.model_json_schema(), indent=2))

    with open(out_base / "TableVersionMetadata.json", "w") as f:
        f.write(json.dumps(TableVersionMetadata.model_json_schema(), indent=2))


if __name__ == "__main__":
    app()
