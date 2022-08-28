import pytest

from dat import generated_tables
from dat.writers.generated_tables_writer import WritePlanBuilder


@pytest.fixture(scope='session')
def plan_builder(spark_session):
    return WritePlanBuilder(
        spark=spark_session
    )


@pytest.fixture(scope='session')
def generated_reference_table_1_write_plan(plan_builder):
    return plan_builder.build_write_plan(
        generated_tables.reference_table_1
    )
