from pyspark.sql import SparkSession



def write_to_hdfs(local_path, spark):
    df = spark.read.csv("file:///"  + local_path, header=True)
    df.write.csv("raw/data.csv", header=True, mode="overwrite")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Data Ingestion") \
    .appName("WriteToHDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:8020") \
    .getOrCreate()
    write_to_hdfs("/data/BigData/ecommerce_data_with_trends.csv", spark)
    spark.stop()
    
