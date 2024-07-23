import delta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

def compare_delta_tables(spark: SparkSession, table_path1: str, table_path2: str):
    # Read Delta tables into DataFrames
    df1 = spark.read.format("delta").load(table_path1)
    df2 = spark.read.format("delta").load(table_path2)

    def failed():
        print("FAILED")
        print(df1)
        print(df2)
    
    # Check schema compatibility (columns and types)
    if df1.schema != df2.schema:
        print("schema mismatch")
        failed()
        return
    
    # Check row-wise equality
    if df1.exceptAll(df2).count() != 0 or df2.exceptAll(df1).count() != 0:
        print("data mismatch")
        failed()
        return


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 3:
        print("Usage: python compare_delta_tables.py <table_path1> <table_path2>")
        sys.exit(1)
    
    table_path1 = sys.argv[1]
    table_path2 = sys.argv[2]

    builder = (
        SparkSession.builder.appName(
            "compare",
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    builder = delta.configure_spark_with_delta_pip(builder)
    spark = builder.getOrCreate()
    
    try:
        compare_delta_tables(spark, table_path1, table_path2)
    finally:
        spark.stop()

