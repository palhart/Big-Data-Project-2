FROM bitnami/spark:latest

COPY docker/data_processing/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

WORKDIR /app
COPY data_processing/data_pipeline.py /app/
COPY data_processing/data_processing.py /app/
COPY data_processing/data_cleaning.py /app/

CMD ["python", "data_pipeline.py"]
