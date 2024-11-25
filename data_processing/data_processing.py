from pyspark.sql import functions as F
from pyspark.sql.types import *

def normalize_timestamp(df):
    return df.withColumn(
        'timestamp',
        F.to_timestamp(F.concat(
            F.current_date(),
            F.lit(' '),
            F.col('timestamp')
        ))
    )

def normalize_categorical_columns(df):
    processed_df = df.withColumn(
        'customer_type',
        F.upper(F.trim(F.col('customer_type')))
    )
    
    processed_df = processed_df.withColumn(
        'category',
        F.regexp_replace('category', '\\s+', ' ')
    )
    
    return processed_df

def filter_invalid_transactions(df):
    return df.filter(
        (F.col('price') > 0) &
        (F.col('quantity') > 0) &
        (F.col('total_amount') > 0) &
        F.col('is_valid_transaction')
    )

def calculate_derived_metrics(df):
    return df.withColumn(
        'unit_price',
        F.round(F.col('total_amount') / F.col('quantity'), 2)
    ).withColumn(
        'transaction_date',
        F.to_date(F.col('timestamp'))
    )

def generate_summary_stats(df):
    print("\nNumerical columns summary:")
    df.select('price', 'quantity', 'total_amount').summary().show()
    
    print("\nTransaction counts by customer type:")
    df.groupBy('customer_type').count().show()

def main_processing_pipeline(df):
    print("Starting processing pipeline...")
    
    processed_df = df
    processed_df = normalize_categorical_columns(processed_df)
    processed_df = filter_invalid_transactions(processed_df)
    processed_df = calculate_derived_metrics(processed_df)
    
    generate_summary_stats(processed_df)
    
    return processed_df