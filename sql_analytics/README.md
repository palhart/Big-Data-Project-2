# Big-Data-Project-2
Customer Data Analysis with Spark

## PySpark SQL Analytics

This feature provides an in-depth analysis of the dataset using PySpark SQL.

### File Structure

- **`sql-analytics/sql_queries.py`**: Contains all the SQL queries used for analysis.
- **`sql-analytics/sql_analytics.py`**: Main script to run the analyses and generate insights.
- **`sql-analytics/load_data.py`**: Contains the function designed to load the data from hdfs.

### How to Run

1. Navigate to the project root directory.
2. Use the following command to execute the analysis:

   ```bash
   python sql-analytics/sql_analytics.py
   ```

### Purpose

This analysis helps identify key insights and trends in the data, such as:

- Top spending customers.
- Product purchase trends over time.
- Revenue breakdown by category.

Feel free to explore and modify the queries in `sql_queries.py` to customize the analysis!