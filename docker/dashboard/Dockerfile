# Use the official Bitnami Spark image as base
FROM bitnami/spark:latest

USER root

# Install necessary build tools and libraries
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    libpq-dev \
    libatlas-base-dev \
    liblapack-dev \
    gfortran \
    && rm -rf /var/lib/apt/lists/*

# Install necessary Python dependencies
RUN pip3 install pandas dash plotly pyspark gunicorn

# Set working directory
WORKDIR /app


# Copy the main script into the container
COPY analytics/ /app/analytics/
COPY application/ /app/application/


CMD [ "gunicorn", "--timeout=300", "--workers=1", "--threads=1", "-b 0.0.0.0:8050", "application.app:server"]
