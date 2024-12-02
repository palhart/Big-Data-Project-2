# E-commerce Data Analysis

## Description

This repository contains the code for loading, preparing, and analyzing e-commerce data from the file `/data/BigData/ecommerce_data_with_trends.csv`. It also includes a Dash-based dashboard for visualizing the analysis results.

---

## Prerequisites

Before using this project, make sure to execute the `data_ingestion/data_ingestion.py` script following the instructions in its README file. This step is necessary to prepare the required data.

---

## Installation

The required libraries should normally be listed in the `pyproject.toml` file and installed automatically. However, if you encounter any issues, ensure you have all necessary dependencies by running:  
```bash
uv add dash pandas plotly pyspark
```

---

## Content

- **`analytics.py`**  
  Provides functions to prepare data, calculate metrics, and analyze purchasing behaviors.

---

### How to view

Check out the README in the `common/` folder to run `app.py` and view the dashboard.