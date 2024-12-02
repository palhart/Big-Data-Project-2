from sql_analytics.sql_queries import *

def get_sql_analytics(spark):
    top_ten_customer = get_top_ten_customer(spark=spark)
    top_five_products = get_top_five_products(spark=spark)
    top_five_cities = get_top_five_cities(spark=spark)
    most_purchased_products = get_most_purchased_products(spark=spark)
    revenue_by_month = get_revenue_by_month(spark=spark)
    less_purchased_products = get_less_purchased_products(spark=spark)
    revenue_contribution = get_revenue_contribution(spark=spark)

    top_ten_customer = top_ten_customer.toPandas()
    top_five_products = top_five_products.toPandas()
    top_five_cities = top_five_cities.toPandas()
    most_purchased_products = most_purchased_products.toPandas()
    revenue_by_month = revenue_by_month.toPandas()
    less_purchased_products = less_purchased_products.toPandas()
    revenue_contribution = revenue_contribution.toPandas()

    return top_ten_customer, top_five_products, top_five_cities, most_purchased_products, revenue_by_month, less_purchased_products, revenue_contribution