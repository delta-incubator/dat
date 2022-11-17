import pytest

from dat import spark_builder


@pytest.fixture(scope='session')
def spark_session(request):
    spark = spark_builder.get_spark_session()
    request.addfinalizer(spark.stop)
    return spark
