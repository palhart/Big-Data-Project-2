from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def write_to_hdfs(local_path, spark):
    df = spark.read.csv("file:///"  + local_path, header=True)
    df.write.csv("raw/data.csv", header=True, mode="overwrite")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Data Ingestion") \
    .appName("WriteToHDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:8020") \
    .getOrCreate()

    logger.info("Spark session is successfully connected.")

    write_to_hdfs("/data/BigData/ecommerce_data_with_trends.csv", spark)

    logger.info("Data is successfully written to HDFS.")

    spark.stop()
    
