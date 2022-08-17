import pytest

from dat import table_definitions
from dat.writers.spark_writer import WritePlanBuilder


@pytest.fixture(scope='session')
def plan_builder(spark_session):
    return WritePlanBuilder(
        spark=spark_session
    )


@pytest.fixture(scope='session')
def reference_table_1_write_plan(plan_builder):
    return plan_builder.build_write_plan(
        table_definitions.reference_table_1
    )
