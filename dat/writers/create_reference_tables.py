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
