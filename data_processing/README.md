# Big-Data-Project-2

## Overview
This project demonstrates a comprehensive data cleaning and processing pipeline for a dataset using PySpark. The pipeline includes the following key steps:

1. **Data Processing and Cleaning**:
   - Handling missing values
   - Removing duplicates
   - Normalizing data formats (timestamp, categorical variables)
   - Validating and cleaning numerical columns
   - Filtering out invalid or irrelevant transactions
   - Generating summary statistics

## Key Challenges
1. **Missing Value Handling**: The dataset did not contain any missing values

3. **Numerical Data Validation**: The pipeline includes checks to ensure that numerical columns (price, quantity, total_amount) contain only non-negative values. This helps to identify and remove any invalid transactions.

## Usage
To run the data pipeline:

1. Ensure you have PySpark installed and configured in your environment.
3. Execute the `data_pipeline.py` script:

```
python data_processing/data_pipeline.py
```

The script will execute the data cleaning and processing pipeline, and print the summary statistics and a sample of the final processed data.
