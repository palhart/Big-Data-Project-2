# Machine Learning Analysis

This folder contains all the resources for performing machine learning analysis, including churn forecasting and customer segmentation. Below, you will find a description of the components in this folder and how they are used.

## Folder Structure

- **`churn.py`**: This Python file contains all the methods and functions for performing churn analysis. These include:
  - Data preprocessing for churn prediction.
  - Feature engineering.
  - Model training and evaluation for churn forecasting.

- **`customer_segmentation.py`**: This Python file includes all the methods and functions for clustering-based customer segmentation. These include:
  - Data preparation and scaling for clustering.
  - Implementation of clustering algorithms (K-Means).

- **Analysis Notebook**: A Jupyter Notebook that ties together the methods from `churn.py` and `customer_segmentation.py`. The notebook is used for:
  - Calling methods from the Python files for performing the churn analysis and segmentation.

## Notebook Usage and Context

The notebook is intended for exploratory analysis and is **not included inside the Docker container**. This is because:
- It serves as a standalone analysis tool for experimentation and detailed computations.
- The focus is on interactivity and visualization, which are not typically part of a productionized application.

---

## Obstacles and Challenges

While developing this analysis, we encountered several challenges, especially during the **feature engineering for customer segmentation**.

### Key Challenges:
**Iterative Process**:
   - We tried multiple combinations of features, scaling methods, and K-Means configurations before achieving satisfactory results.
   - Each iteration involved careful evaluation of cluster quality using metrics like silhouette score.

### Outcome:
After extensive trials and errors, we managed to produce **well-defined clusters** that provide actionable insights into customer segmentation. These results significantly enhance the understanding of customer behavior and allow for targeted marketing or resource allocation.