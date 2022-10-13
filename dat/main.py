import json
import logging
import os
import sys
from pathlib import Path

import click

from dat import external_tables, generated_tables, spark_builder
from dat.model.table import ReferenceTable
from dat.writers import create_reference_tables, external_tables_writer, generated_tables_writer

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

    - generating tables metadata in json for external tables

    - generating json schemas of the metadata for code generation
        in other programming languages


    """
    pass  # noqa: WPS420


# @click.command()
# @click.option(
#     '--table-names',
#     default='all',
#     help='The reference table names to create. Can be a comma separated list or all',  # noqa: E501
# )
# @click.option(
#     '--output-path',
#     default='./out/tables/generated',
#     help='The base folder where the tables should be written',
# )
# def write_generated_reference_tables(table_names, output_path):
#     logging.info(
#         'Writing tables to {output_path} using filter={filter}'.format(
#             output_path=output_path,
#             filter=table_names,
#         ),
#     )
#     tables = generated_tables.get_tables(
#         table_names,
#     )
#     spark = spark_builder.create_spark_session()
#     generated_tables_writer.write_generated_tables(
#         spark,
#         output_path,
#         tables,
#     )
#     logging.info('Generated reference tables successfully written')


@click.command()
def write_generated_reference_tables():
    create_reference_tables.create_reference_table_1()
    create_reference_tables.create_reference_table_2()
    create_reference_tables.create_reference_table_3()


@click.command()
@click.option(
    '--output-path',
    default='./out/schemas',
    help='The base folder where the schema should be written',
)
def write_schemas(output_path):
    os.makedirs(output_path, exist_ok=True)
    file_name = '{path}/schema.json'.format(
        path=output_path,
    )
    logging.info(
        'Writing Json Schema of table metadata to {out}'.format(
            out=file_name,
        ),
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
    default='./external-tables',
    help='The path where external tables should be found',
)
@click.option(
    '--output-path',
    default='./out/tables/external',
    help='The path where external tables should be found',
)
def create_external_tables(external_tables_path, output_path):
    os.makedirs(output_path, exist_ok=True)
    try:
        external_tables_writer.write_external_tables(
            external_tables_data_path=Path(external_tables_path),
            tables=external_tables.all_tables,
            output_path=Path(output_path),
        )
    except ValueError as err:
        logging.info(
            'Error when writing metadata for external tables',
        )
        logging.info(err)
        sys.exit(-1)


cli.add_command(write_generated_reference_tables)
cli.add_command(write_schemas)
cli.add_command(create_external_tables)

if __name__ == '__main__':
    cli()
