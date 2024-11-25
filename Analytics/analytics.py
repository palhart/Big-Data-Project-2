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

def customer_demographics_analysis(df):
    customer_type_dist = df['customer_type'].value_counts()
    city_dist = df['city'].value_counts().head(10)
    category_dist = df['category'].value_counts()
    return customer_type_dist, city_dist, category_dist

def purchasing_patterns(df):
    hourly_sales = df.groupby('hour')['total_amount'].mean()
    daily_sales = df.groupby('day_of_week')['total_amount'].mean()
    monthly_sales = df.groupby('month')['total_amount'].mean()
    return hourly_sales, daily_sales, monthly_sales