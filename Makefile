build: 
	docker build -f docker/ingestion/Dockerfile -t pyspark-hdfs-writer .
	docker build -f docker/data_processing/Dockerfile -t pyspark-data-processing .
run: 
	docker compose up -d 



