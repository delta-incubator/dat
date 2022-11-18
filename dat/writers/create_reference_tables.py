import pyspark
from delta import *
from pyspark.sql.types import *
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

def create_reference_table_1():
    columns = ["letter", "number", "a_float"]
    
    data = [('a', 1, 1.1), ("b", 2, 2.2), ("c", 3, 3.3)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)

    delta_table_path = "out/tables/generated/reference_table_1/delta"
    expected_path = "out/tables/generated/reference_table_1/expected"
    os.makedirs(expected_path)

    df.repartition(1).write.format("delta").save(delta_table_path)
    actual0 = spark.read.format("delta").load(delta_table_path)
    os.makedirs(f"{expected_path}/v0")
    actual0.toPandas().to_parquet(f"{expected_path}/v0/table_content.parquet")

    data = [("d", 4, 4.4), ("e", 5, 5.5)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)    

    df.repartition(1).write.format("delta").mode("append").save(delta_table_path)
    actual1 = spark.read.format("delta").load(delta_table_path)
    os.makedirs(f"{expected_path}/v1")
    actual1.toPandas().to_parquet(f"{expected_path}/v1/table_content.parquet")
    os.makedirs(f"{expected_path}/latest")
    actual1.toPandas().to_parquet(f"{expected_path}/latest/table_content.parquet")


def create_reference_table_2():
    columns = ["letter", "number", "a_float"]
    
    data = [('a', 1, 1.1), ("b", 2, 2.2), ("c", 3, 3.3)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)

    delta_table_path = "out/tables/generated/reference_table_2/delta"
    expected_path = "out/tables/generated/reference_table_2/expected"
    os.makedirs(expected_path)

    df.repartition(1).write.partitionBy("letter").format("delta").save(delta_table_path)
    actual0 = spark.read.format("delta").load(delta_table_path)
    os.makedirs(f"{expected_path}/v0")
    actual0.toPandas().to_parquet(f"{expected_path}/v0/table_content.parquet")

    data = [("a", 4, 4.4), ("e", 5, 5.5)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)    

    df.repartition(1).write.partitionBy("letter").format("delta").mode("append").save(delta_table_path)
    actual1 = spark.read.format("delta").load(delta_table_path)
    os.makedirs(f"{expected_path}/v1")
    actual1.toPandas().to_parquet(f"{expected_path}/v1/table_content.parquet")
    os.makedirs(f"{expected_path}/latest")
    actual1.toPandas().to_parquet(f"{expected_path}/latest/table_content.parquet")


def create_reference_table_3():
    columns = ["letter", "number"]
    
    data = [("a", 1), ("b", 2), ("c", 3)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)

    delta_table_path = "out/tables/generated/reference_table_3/delta"
    expected_path = "out/tables/generated/reference_table_3/expected"

    df.repartition(1).write.format("delta").save(delta_table_path)
    actual0 = spark.read.format("delta").load(delta_table_path)
    os.makedirs(f"{expected_path}/v0")
    actual0.toPandas().to_parquet(f"{expected_path}/v0/table_content.parquet")

    data = [("d", 4), ("e", 5)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)    

    df.repartition(1).write.format("delta").mode("append").save(delta_table_path)
    actual1 = spark.read.format("delta").load(delta_table_path)
    os.makedirs(f"{expected_path}/v1")
    actual1.toPandas().to_parquet(f"{expected_path}/v1/table_content.parquet")

    data = [("x", 66), ("y", 77)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)    

    df.repartition(1).write.format("delta").mode("overwrite").save(delta_table_path)
    actual2 = spark.read.format("delta").load(delta_table_path)
    os.makedirs(f"{expected_path}/v2")
    actual2.toPandas().to_parquet(f"{expected_path}/v2/table_content.parquet")
    os.makedirs(f"{expected_path}/latest")
    actual2.toPandas().to_parquet(f"{expected_path}/latest/table_content.parquet")


def create_reference_table_4():
    # name: reference_table_4
    # description: A table which has had a schema change with overwriteSchema set to True
    columns = ["letter", "number"]
    data = [("a", 1), ("b", 2), ("c", 3)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)

    delta_table_path = "out/tables/generated/reference_table_4/delta"
    expected_path = "out/tables/generated/reference_table_4/expected"

    df.repartition(1).write.format("delta").save(delta_table_path)
    actual0 = spark.read.format("delta").load(delta_table_path)
    os.makedirs(f"{expected_path}/v0")
    actual0.toPandas().to_parquet(f"{expected_path}/v0/table_content.parquet")

    columns = ["num1", "num2"]
    data = [(22, 33), (44, 55), (66, 77)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)

    df.repartition(1).write.mode("overwrite").option("overwriteSchema", True).format("delta").save(delta_table_path)
    actual1 = spark.read.format("delta").load(delta_table_path)
    os.makedirs(f"{expected_path}/v1")
    actual1.toPandas().to_parquet(f"{expected_path}/v1/table_content.parquet")
    os.makedirs(f"{expected_path}/latest")
    actual1.toPandas().to_parquet(f"{expected_path}/latest/table_content.parquet")


def create_writer_reference_table_1():
    columns = ["letter", "number", "a_float"]
    
    data = [("a", 1, 1.1), ("b", 2, 2.2), ("c", 3, 3.3)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)

    delta_table_path = "out/tables/writer/reference_table_1/delta"
    expected_path = "out/tables/writer/reference_table_1/expected"
    other_path = "out/tables/writer/reference_table_1/other"
    os.makedirs(expected_path)
    os.makedirs(other_path)

    df.repartition(1).write.format("delta").save(delta_table_path)

    data = [("d", 4, 4.4), ("e", 5, 5.5)]
    rdd = spark.sparkContext.parallelize(data)
    df = rdd.toDF(columns)

    # data that will be appended in the integration test
    rdd = spark.sparkContext.parallelize([("z", 9, 9.9)])
    append_df = rdd.toDF(["letter", "number", "a_float"])
    append_df.toPandas().to_parquet(f"{other_path}/append_content_1.parquet")

    df.repartition(1).write.format("delta").mode("append").save(delta_table_path)
    os.makedirs(f"{expected_path}/post_append")
    actual = spark.read.format("delta").load(delta_table_path)
    actual.union(append_df).toPandas().to_parquet(f"{expected_path}/post_append/table_content.parquet")
