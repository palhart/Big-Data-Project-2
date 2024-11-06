from hdfs import InsecureClient
import pandas as pd

# Connect to the NameNode's WebHDFS interface
hdfs_client = InsecureClient('http://localhost:9870', user='paul')


def upload_file_to_hdfs(local_file_path, hdfs_file_path):
    try:
        hdfs_client.upload(hdfs_file_path, local_file_path, overwrite=True)
        print(f"Successfully uploaded {local_file_path} to {hdfs_file_path}")
    except Exception as e:
        print(f"Failed to upload {local_file_path} to {hdfs_file_path}: {e}")



