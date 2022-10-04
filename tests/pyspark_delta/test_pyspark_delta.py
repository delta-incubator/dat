import pytest
import chispa
import pyspark
from delta import *

builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

def test_reference_table_1_content():
    actual_df = spark.read.format("delta").load("out/tables/generated/reference_table_1/delta")
    expected_df = spark.read.format("parquet").load("out/tables/generated/reference_table_1/parquet")
    chispa.assert_df_equality(actual_df, expected_df)