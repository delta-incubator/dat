import pytest
import chispa
import pyspark
from delta import *
import shutil
import os

builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

def test_reference_table_1_append():
    os.makedirs("tmp")

    # copy over existing reference table
    shutil.copytree("out/tables/writer/reference_table_1", "tmp/reference_table_1")
    df = spark.read.format("delta").load("tmp/reference_table_1/delta")
    
    # append data to the reference table
    df = spark.read.format("parquet").load("tmp/reference_table_1/other/append_content_1.parquet")
    df.write.format("delta").mode("append").save("tmp/reference_table_1/delta")

    # fetch expected content  
    expected_df = spark.read.format("parquet").load("tmp/reference_table_1/expected/post_append/table_content.parquet")

    # make assertions & cleanup
    actual_df = spark.read.format("delta").load("tmp/reference_table_1/delta")
    chispa.assert_df_equality(actual_df, expected_df, ignore_row_order=True)
    shutil.rmtree('tmp')