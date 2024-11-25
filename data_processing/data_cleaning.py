from pyspark.sql import functions as F
from pyspark.sql.types import *

def check_missing_values(df):
    missing_counts = []
    for column in df.columns:
        print("Checking missing values for column:", column)
        if column != 'timestamp':
            missing_count = df.filter(
                F.col(column).isNull() | 
                F.isnan(column) | 
                (F.col(column) == '') | 
                (F.col(column) == 'NULL')
            ).count()
        missing_counts.append((column, missing_count))
    
    print("\nMissing value counts:")
    for col, count in missing_counts:
        print(f"{col}: {count}")
    
    return missing_counts

def fill_missing_values(df):
    return df.na.fill({
        'quantity': 0,
        'price': 0.0,
        'total_amount': 0.0,
        'city': 'Unknown',
        'customer_name': 'Unknown',
        'category': 'Uncategorized'
    })

def remove_critical_nulls(df, critical_columns):
    return df.dropna(subset=critical_columns)

def remove_duplicates(df, key_columns):
    print("Removing duplicates based on columns:", key_columns)

    nb_rows_before = df.count()
    df = df.dropDuplicates(key_columns)
    nb_rows_after = df.count()

    print(f"Removed {nb_rows_before - nb_rows_after} duplicates.")
    return df.dropDuplicates(key_columns)

def clean_numerical_values(df):
    return df.withColumn(
        'price',
        F.when(F.col('price') < 0, 0).otherwise(F.col('price'))
    ).withColumn(
        'quantity',
        F.when(F.col('quantity') < 0, 0).otherwise(F.col('quantity'))
    ).withColumn(
        'total_amount',
        F.when(F.col('total_amount') < 0, 0).otherwise(F.col('total_amount'))
    )

def validate_transactions(df):
    return df.withColumn(
        'is_valid_transaction',
        (F.abs(F.col('price') * F.col('quantity') - F.col('total_amount')) <= 0.01)
    )

def main_cleaning_pipeline(df):
    print("Starting cleaning pipeline...")
    print("Initial row count before cleaning:", df.count())
    
    missing_counts = check_missing_values(df)
    cleaned_df = df
    if any([count > 0 for col, count in missing_counts]):
        print("Removing rows with critical missing values...")
        cleaned_df = remove_critical_nulls(cleaned_df, ['transaction_id', 'customer_id', 'customer_type'])
        print("Filling missing values...")
        cleaned_df = fill_missing_values(df)
    else:
        print("No missing values found.")
        print("Skipping remove critical nulls step.")
        print("Skipping fill missing values step.")

    cleaned_df = remove_duplicates(cleaned_df, ['transaction_id'])
    cleaned_df = clean_numerical_values(cleaned_df)
    cleaned_df = validate_transactions(cleaned_df)
    
    print("Final row count after cleaning:", cleaned_df.count())
    return cleaned_df