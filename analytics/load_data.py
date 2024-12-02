import os
from pyspark.sql import SparkSession

def load_data():
    spark = SparkSession.builder \
        .appName("E-commerce Analytics") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:8020") \
        .getOrCreate()

    input_file_path = "/user/spark/processed/data.csv"

    spark_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_file_path)
        
    df = spark_df.toPandas()
    # Clean up
    spark.stop()
    return df