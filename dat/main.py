import json
import logging
import os

import click

from dat import spark_builder, table_definitions
from dat.model.table import ReferenceTable
from dat.writers import spark_writer

logging.basicConfig(
    level=logging.INFO
)


@click.group()
def cli():
    pass


@click.command()
@click.option(
    '--table-names',
    default='all',
    help='The reference table names to create. Can be a comma separated list or all'  # noqa: E501
)
@click.option(
    '--output-path',
    default='./out/tables',
    help='The base folder where the tables should be written'
)
def write_reference_tables(table_names, output_path):
    logging.info(
        'Writing tables to {output_path} using filter={filter}'.format(
            output_path=output_path,
            filter=table_names,
        )
    )
    reference_tables = table_definitions.get_tables(
        table_names
    )
    spark = spark_builder.create_spark_session()
    write_plan_builder = spark_writer.WritePlanBuilder(
        spark=spark
    )
    write_plans = map(
        lambda table: write_plan_builder.build_write_plan(table),
        reference_tables
    )
    for write_plan in write_plans:
        logging.info(
            'Writing {table_name}'.format(
                table_name=write_plan.table.table_name
            )
        )
        os.makedirs(output_path, exist_ok=True)
        spark_writer.write(
            spark,
            write_plan,
            output_path
        )
    logging.info('Reference table successfully written')


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


cli.add_command(write_reference_tables)
cli.add_command(write_schemas)

if __name__ == '__main__':
    cli()
