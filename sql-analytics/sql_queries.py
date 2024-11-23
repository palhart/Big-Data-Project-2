def get_top_ten_customer(spark):
    # Query to find the 10 customers that spent the most
    query = """
    SELECT customer_id, customer_name, SUM(total_amount) AS total_spent
    FROM transactions
    GROUP BY customer_id, customer_name
    ORDER BY total_spent DESC
    LIMIT 10
    """
    return spark.sql(query)