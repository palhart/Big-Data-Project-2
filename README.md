# PySpark Data Processing and Visualization Dashboard

![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

## 📌 Project Overview

This repository provides a robust data processing and visualization solution using PySpark, Docker, and a web dashboard. The application offers a complete pipeline for data ingestion, transformation, and interactive visualization.

## 🚀 Features

- **Dockerized Microservices Architecture**
- **PySpark-powered Data Processing**
- **Scalable Data Ingestion**
- **Interactive Web Dashboard**
- **Containerized Deployment**


## 📦 Project Structure

```
.
├── Makefile
├── README.md
├── analytics
│   ├── README.md
│   ├── analytics.py
│   └── load_data.py
├── application_photo_and_ML_html
│   ├── App_image_1.png
│   ├── App_image_10.png
│   ├── App_image_2.png
│   ├── App_image_3.png
│   ├── App_image_4.png
│   ├── App_image_5.png
│   ├── App_image_6.png
│   ├── App_image_7.png
│   ├── App_image_8.png
│   ├── App_image_9.png
│   └── ml_analysis.html
├── common
│   ├── README.md
│   └── app.py
├── data_ingestion
│   └── data_ingestion.py
├── data_processing
│   ├── README.md
│   ├── data_cleaning.py
│   ├── data_pipeline.py
│   └── data_processing.py
├── docker
│   ├── dashboard
│   │   └── Dockerfile
│   ├── data_processing
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── ingestion
│       ├── Dockerfile
│       └── requirements.txt
├── docker-compose.yml
├── hadoop-config
│   └── config.env
├── ml_module
│   ├── README.md
│   ├── churn.py
│   ├── customer_segmentation.py
│   └── ml_analysis.ipynb
└── sql-analytics
    ├── README.md
    ├── load_data.py
    ├── sql_analytics.py
    └── sql_queries.py

```

## 🔧 Installation and Setup

### 1. Build Services

```bash
# Using Make (recommended)
make build

# Alternative Docker Compose command
docker-compose build
```

### 3. Run the Application

```bash
# Using Make
make run

# Alternative Docker Compose command
docker-compose up -d
```
### 4. Acces HDFS database 

Open yout web browser and navigate to: 
- HDFS URL: `http://localhost:9870/`


### 5. Access the Dashboard

Open your web browser and navigate to:
- Dashboard URL: `http://localhost:8050`

## 📋 Makefile Commands

| Command | Description |
|---------|-------------|
| `make build` | Build all Docker images |
| `make run` | Start all services |
| `make stop` | Stop all running services |
| `make rm` | Remove containers and images |

## 🔬 Services

### 1. Ingestion Service
- Processes raw data
- Writes data to distributed storage
- Uses PySpark for efficient data handling

### 2. Data Processing Service
- Performs advanced data transformations
- Applies business logic and data cleaning
- Prepares data for visualization
- More information : [Data Processing](data_processing/README.md)

### 3. Dashboard Service
- In-depth analysis of the dataset using PySpark SQL
- Web-based interactive dashboard
- Real-time data visualization
- Responsive design
- More information: [Dashboard](analytics/README.md) and [Sql Analytics](sql_analytics/README.md)

### 4. Machine Learning
- Use Spark ML to make some ML on the dataset
- Add churn forecasting and customer segmentation
- Not included inside the Docker container
- More information: [Machine Learning](ml_module/README.md)

### 5. Application photo and ML html
- Screenshots of the application with all analysis
- HTML of the notebook from the ml_module folder

## 📚 Resources

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/index.html)
- [Docker Documentation](https://docs.docker.com/)
- [Plotly Dash Documentation](https://dash.plotly.com/)
