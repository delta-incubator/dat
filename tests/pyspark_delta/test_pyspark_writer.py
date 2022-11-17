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
    shutil.copytree("out/tables/generated/reference_table_1/delta", "tmp/delta")
    df = spark.read.format("delta").load("tmp/delta")
    # +------+------+-------+                                                         
    # |letter|number|a_float|
    # +------+------+-------+
    # |     a|     1|    1.1|
    # |     b|     2|    2.2|
    # |     c|     3|    3.3|
    # |     d|     4|    4.4|
    # |     e|     5|    5.5|
    # +------+------+-------+
    
    # append data to the reference table
    rdd = spark.sparkContext.parallelize([("z", 9, 9.9)])
    df = rdd.toDF(["letter", "number", "a_float"])
    df.write.format("delta").mode("append").save("tmp/delta")

    # created expected content
    rdd = spark.sparkContext.parallelize([
        ("a", 1, 1.1),
        ("b", 2, 2.2),
        ("c", 3, 3.3),
        ("d", 4, 4.4),
        ("e", 5, 5.5),
        ("z", 9, 9.9),
    ])
    expected_df = rdd.toDF(["letter", "number", "a_float"])    

    # make assertions & cleanup
    actual_df = spark.read.format("delta").load("tmp/delta")
    chispa.assert_df_equality(actual_df, expected_df, ignore_row_order=True)
    shutil.rmtree('tmp')