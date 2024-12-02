build: 
	docker build -f docker/ingestion/Dockerfile -t pyspark-hdfs-writer .
	docker build -f docker/data_processing/Dockerfile -t pyspark-data-processing .
	docker build -f docker/dashboard/Dockerfile -t dashboard .
run: 
	docker compose up 

rm:
	docker compose down --rmi all --volumes --remove-orphans



