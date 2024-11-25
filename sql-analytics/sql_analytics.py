import os
from pyspark.sql import SparkSession

from load_data import load_data
from sql_queries import *
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

data = load_data(spark=spark)

# Register the DataFrame as a temporary view
data.createOrReplaceTempView("transactions")

# Exemple of how to get the analysis with SQL
top_ten_customer_df = get_top_ten_customer(spark=spark)

top_five_products_df = get_top_five_products(spark=spark)

top_five_cities_df = get_top_five_cities(spark=spark)