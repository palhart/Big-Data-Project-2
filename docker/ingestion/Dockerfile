# Use the official Bitnami Spark image as base
FROM bitnami/spark:latest

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY docker/ingestion/requirement.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the main script into the container
COPY data_ingestion/data_ingestion.py /app/data_ingestion.py

# Define the command to run the script
CMD ["python", "data_ingestion.py"]
