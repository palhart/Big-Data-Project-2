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

def get_top_five_products(spark):
    # Query to find the 5 most popular products by quantity sold
    query = """
    SELECT product_name, SUM(quantity) AS total_quantity
    FROM transactions
    GROUP BY product_name
    ORDER BY total_quantity DESC
    LIMIT 5
    """
    return spark.sql(query) 

def get_top_five_cities(spark):
    # Query to find the top 5 cities with the highest total spending
    query = """
    SELECT city, SUM(total_amount) AS total_spent
    FROM transactions
    WHERE city IS NOT NULL AND total_amount IS NOT NULL
    GROUP BY city
    ORDER BY total_spent DESC
    LIMIT 5
    """
    return spark.sql(query) 