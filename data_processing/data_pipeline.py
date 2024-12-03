from data_cleaning import main_cleaning_pipeline
from data_processing import main_processing_pipeline
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session(app_name, default_fs):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.defaultFS", default_fs) \
        .getOrCreate()

def load_data(spark, file_path):
    logger.info("Loading data from HDFS...")
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)

def run_data_pipeline(spark_df):
    logger.info("Running data cleaning pipeline...")
    cleaned_df = main_cleaning_pipeline(spark_df)
    
    logger.info("Running data processing pipeline...")
    final_df = main_processing_pipeline(cleaned_df)
    
    final_df.cache()
    logger.info("Data pipeline completed successfully.")
    return final_df

def save_to_hdfs(df, output_path):
    logger.info(f"Saving processed data to {output_path}...")
    df.write.csv(output_path, header=True, mode="overwrite")
    logger.info(f"Data successfully written to {output_path}")

def main():
    app_name = "Data Processing and Cleaning"
    default_fs = "hdfs://hdfs-namenode:8020"
    input_file_path = "/user/spark/raw/data.csv"
    hdfs_input_path = default_fs + input_file_path

    spark = create_spark_session(app_name, default_fs)
    logger.info("Spark session successfully initialized.")

    spark_df = load_data(spark, hdfs_input_path)
    logger.info("Data successfully loaded into Spark DataFrame.")

    final_df = run_data_pipeline(spark_df)

    hdfs_output_path = "/user/spark/processed/data.csv"
    save_to_hdfs(final_df, default_fs + hdfs_output_path)

    print("\nSample of final processed data:")
    final_df.show(5)

if __name__ == "__main__":
    main()
