import json
import logging
import os
import sys

import click

from dat import external_tables, generated_tables, spark_builder
from dat.model.table import ReferenceTable
from dat.writers import generated_tables_writer, metadata_writer

logging.basicConfig(
    level=logging.INFO
)


@click.group()
def cli():
    """DAT (Delta Acceptance Testing) CLI helper

        This CLI tool helps performing mundane tasks related to managing
        reference tables for delta acceptance testing, including:

        - generating tables from python code

        - generating tables metadata in json for external tables

        - generating json schemas of the metadata for code generation
        in other programming languages

    """
    pass


@click.command()
@click.option(
    '--table-names',
    default='all',
    help='The reference table names to create. Can be a comma separated list or all'  # noqa: E501
)
@click.option(
    '--output-path',
    default='./out/tables/generated',
    help='The base folder where the tables should be written'
)
def write_generated_reference_tables(table_names, output_path):
    logging.info(
        'Writing tables to {output_path} using filter={filter}'.format(
            output_path=output_path,
            filter=table_names,
        )
    )
    tables = generated_tables.get_tables(
        table_names
    )
    spark = spark_builder.create_spark_session()
    generated_tables_writer.write_generated_tables(
        spark,
        output_path,
        tables
    )
    logging.info('Generated reference tables successfully written')


@click.command()
@click.option(
    '--output-path',
    default='./out/schemas',
    help='The base folder where the schema should be written'
)
def write_schemas(output_path):
    os.makedirs(output_path, exist_ok=True)
    file_name = '{path}/schema.json'.format(
        path=output_path
    )
    with open(file_name, 'w') as outfile:
        json.dump(
            ReferenceTable.schema(),
            fp=outfile,
            indent=2,
        )


@click.command()
@click.option(
    '--external-tables-path',
    default='./out/tables/external',
    help='The path where external tables should be found'
)
def write_external_tables_metadata(external_tables_path):
    os.makedirs(external_tables_path, exist_ok=True)
    subfolders = os.listdir(external_tables_path)
    subfolders_abspath = [
        os.path.join(
            external_tables_path, subfolder
        )
        for subfolder in subfolders
    ]
    try:
        metadata_writer.write_all_metadata(
            subfolders_abspath,
            external_tables.all
        )
    except ValueError as e:
        click.echo(
            'Error when writing metadata for external tables'
        )
        click.echo(e)
        sys.exit(-1)


cli.add_command(write_generated_reference_tables)
cli.add_command(write_schemas)
cli.add_command(write_external_tables_metadata)

if __name__ == '__main__':
    cli()
