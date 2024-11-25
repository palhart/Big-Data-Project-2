from data_cleaning import main_cleaning_pipeline
from data_processing import main_processing_pipeline
from pyspark.sql import SparkSession
import os


def run_data_pipeline(spark_df):
    cleaned_df = main_cleaning_pipeline(spark_df)
    final_df = main_processing_pipeline(cleaned_df)
    final_df.cache()
    
    print("Data pipeline completed successfully!")
    return final_df


sparkSession = SparkSession.builder \
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
spark_df = sparkSession.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(local_file_path)

final_df = run_data_pipeline(spark_df)

# Show sample of final data
print("\nSample of final processed data:")
final_df.show(5)