build: 
	docker build -f docker/ingestion/Dockerfile -t pyspark-hdfs-writer .
run: 
	docker compose up -d 



