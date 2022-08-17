import delta
from pyspark.sql import SparkSession


def create_spark_session():
    builder = SparkSession.builder.appName(
        'DAT'
    ).config(
        'spark.sql.extensions',
        'io.delta.sql.DeltaSparkSessionExtension'
    ).config(
        'spark.sql.catalog.spark_catalog',
        'org.apache.spark.sql.delta.catalog.DeltaCatalog',
    )
    return delta.configure_spark_with_delta_pip(builder).getOrCreate()
