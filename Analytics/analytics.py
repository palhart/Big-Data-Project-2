import pandas as pd

def prepare_data(df):
    df['date'] = pd.to_datetime(df['timestamp']).dt.date
    df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
    df['month'] = pd.to_datetime(df['timestamp']).dt.month
    df['day_of_week'] = pd.to_datetime(df['timestamp']).dt.day_name()
    return df

def calculate_key_metrics(df):
    total_revenue = df['total_amount'].sum()
    total_customers = df['customer_id'].nunique()
    total_transactions = len(df)
    avg_order_value = total_revenue / total_transactions
    return total_revenue, total_customers, total_transactions, avg_order_value