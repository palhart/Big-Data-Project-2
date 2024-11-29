def get_top_ten_customer(spark):
    # Query to find the 100 customers that spent the most
    query = """
    SELECT customer_id, customer_name, SUM(total_amount) AS total_spent
    FROM transactions
    GROUP BY customer_id, customer_name
    ORDER BY total_spent DESC
    LIMIT 100
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

def get_most_purchased_products(spark):
    query = """
    SELECT product_name, SUM(quantity) AS total_quantity 
    FROM transactions 
    GROUP BY product_name 
    ORDER BY total_quantity DESC
    LIMIT 100
    """
    return spark.sql(query) 

def get_revenue_by_month(spark):
    query = """
    SELECT DATE_FORMAT(timestamp, 'yyyy-MM') AS month, SUM(total_amount) AS total_revenue 
    FROM transactions 
    GROUP BY month 
    ORDER BY month
    """
    return spark.sql(query) 

def get_less_purchased_products(spark):
    query = """
    SELECT product_name, SUM(quantity) AS total_quantity 
    FROM transactions 
    GROUP BY product_name 
    ORDER BY total_quantity ASC
    LIMIT 100
    """
    return spark.sql(query) 

def get_revenue_contribution(spark):
    query = """
    SELECT SUBSTRING_INDEX(category, '>', 1) AS main_category,
    SUM(total_amount) AS revenue
    FROM transactions
    GROUP BY main_category
    ORDER BY revenue DESC;
    """
    return spark.sql(query) 


def get_most_purchased_category_by_area(spark, area_name):
    # Replace with the actual query to get the most purchased category based on area
    query = f"""
    SELECT category, SUM(total_amount) as total_spent
    FROM transactions
    WHERE city = '{area_name}'
    GROUP BY category
    ORDER BY total_spent DESC
    LIMIT 1
    """
    
    return spark.sql(query) 