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
    versions = [0, 1]
    for v in versions:
        actual_df = spark.read.format("delta").option("versionAsOf", v).load("out/tables/generated/reference_table_1/delta")
        expected_df = spark.read.format("parquet").load(f"out/tables/generated/reference_table_1/expected/v{v}/table_content.parquet")
        chispa.assert_df_equality(actual_df, expected_df)


def test_reference_table_2_content():
    versions = [0, 1]
    for v in versions:
        actual_df = spark.read.format("delta").option("versionAsOf", v).load("out/tables/generated/reference_table_2/delta")
        expected_df = spark.read.format("parquet").load(f"out/tables/generated/reference_table_2/expected/v{v}/table_content.parquet")
        chispa.assert_df_equality(actual_df, expected_df)


def test_reference_table_3_content():
    versions = [0, 1, 2]
    for v in versions:
        actual_df = spark.read.format("delta").option("versionAsOf", v).load("out/tables/generated/reference_table_3/delta")
        expected_df = spark.read.format("parquet").load(f"out/tables/generated/reference_table_3/expected/v{v}/table_content.parquet")
        chispa.assert_df_equality(actual_df, expected_df)


def test_all_readers():
    test_config = [
        {"rt": 1, "versions": [0, 1]},
        {"rt": 2, "versions": [0, 1]},
        {"rt": 3, "versions": [0, 1, 2]},
    ]
    for config in test_config:
        for v in config["versions"]:  
            rt_num = config["rt"]
            actual_df = spark.read.format("delta").option("versionAsOf", v).load(f"out/tables/generated/reference_table_{rt_num}/delta")
            expected_df = spark.read.format("parquet").load(f"out/tables/generated/reference_table_{rt_num}/expected/v{v}/table_content.parquet")
            chispa.assert_df_equality(actual_df, expected_df)
