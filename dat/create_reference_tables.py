# script is run with the pyspark-322-delta-200 environment
# create the env with this command: conda env create -f envs/pyspark-322-delta-200
# activate the environment with this command: conda activate pyspark-322-delta-200

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


def create_reference_table1():
    column_names = ["letter", "number", "a_float"]
    data = [("a", 1, 1.1), ("b", 2, 2.2), ("c", 3, 3.3)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(column_names)

    df.write.format("delta").save("reference_tables/reference_table1/table")

    data = [("d", 4, 4.4), ("e", 5, 5.5)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(column_names)

    df.write.mode("append").format("delta").save(
        "reference_tables/reference_table1/table"
    )

    df = spark.read.format("delta").load("reference_tables/reference_table1/table")

    df.toPandas().to_parquet(
        "reference_tables/reference_table1/table_content.parquet"
    )

    # TODO: Create the table_metadata.json file

    # Structure of table_metadata.json
    #     table name: String
    #     reader_protocol_version: integer
    #     writer_protocol_version: integer
    #     (optional) schema: Nested json
    #     (optional) features: array-of-string


def create_reference_table2():
    column_names = ["letter", "number", "a_float"]
    data = [("a", 1, 1.1), ("a", 2, 2.2), ("c", 3, 3.3)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(column_names)

    df.write.partitionBy("letter").format("delta").save(
        "reference_tables/reference_table2/table"
    )

    data = [("a", 4, 4.4), ("e", 5, 5.5)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(column_names)

    df.write.partitionBy("letter").mode("append").format("delta").save(
        "reference_tables/reference_table2/table"
    )

    df = spark.read.format("delta").load("reference_tables/reference_table2/table")

    df.toPandas().to_parquet(
        "reference_tables/reference_table2/table_content.parquet"
    )

    # TODO: Create the table_metadata.json file


create_reference_table1()
create_reference_table2()
