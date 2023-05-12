import delta
from pyspark.sql import SparkSession

builder = None


def get_spark_session():
    global builder
    # Only configure the builder once
    if builder is None:
        builder = SparkSession.builder.appName(
            'DAT',
        ).config(
            'spark.sql.extensions',
            'io.delta.sql.DeltaSparkSessionExtension',
        ).config(
            'spark.sql.catalog.spark_catalog',
            'org.apache.spark.sql.delta.catalog.DeltaCatalog',
        )
        builder = delta.configure_spark_with_delta_pip(builder)
    spark = builder.enableHiveSupport().getOrCreate()
    spark._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    # spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    return spark
