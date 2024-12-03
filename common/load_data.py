import os
from pyspark.sql import SparkSession

def load_data():
    spark = SparkSession.builder \
        .appName("E-commerce Analytics") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.default.parallelism", "4") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.warehouse.dir", os.path.abspath('spark-warehouse')) \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .getOrCreate()
    local_file_path = "/data/BigData/ecommerce_data_with_trends.csv"

    spark_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(local_file_path)
    spark_df.createOrReplaceTempView("transactions")
    
    df = spark_df.toPandas()

    return df, spark