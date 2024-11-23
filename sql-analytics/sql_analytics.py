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


result = get_top_ten_customer(spark=spark)

# Show the result
result.show()

result = get_top_five_products(spark=spark)

# Show the result
result.show()

result = get_top_five_cities(spark=spark)

# Show the result
result.show()