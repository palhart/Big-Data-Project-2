import os
from pyspark.sql import SparkSession

def load_data(spark):
    local_file_path = "/data/BigData/ecommerce_data_with_trends.csv"
    spark_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(local_file_path)
    return spark_df