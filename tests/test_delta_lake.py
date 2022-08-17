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


def test_reference_table1_a():
    df = spark.read.format("delta").load("./reference_tables/reference_table1/table")
    expected_df = spark.read.parquet("./reference_tables/reference_table1/table_content.parquet")
    chispa.assert_df_equality(df, expected_df)


def test_reference_table2_a():
    df = spark.read.format("delta").load("./reference_tables/reference_table2/table")
    expected_df = spark.read.parquet("./reference_tables/reference_table2/table_content.parquet")
    chispa.assert_df_equality(df, expected_df)