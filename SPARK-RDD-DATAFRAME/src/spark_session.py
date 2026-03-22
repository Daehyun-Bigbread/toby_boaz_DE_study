from pyspark.sql import SparkSession

def create_spark():
    spark = (
        SparkSession.builder
        .appName("RDD_vs_DataFrame")
        .master("local[*]")
        .getOrCreate()
    )
    return spark