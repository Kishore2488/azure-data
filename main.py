"""
Spark SQL Demo Application

This script demonstrates:
- Loading CSV data into Spark DataFrames
- Registering DataFrames as SQL views
- Executing SQL queries with joins, aggregations, and filtering
"""

from pyspark.sql import SparkSession
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SparkSQL_Demo") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Set log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Get the absolute path to the data directory
current_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(current_dir, "data")

print("=" * 80)
print("SPARK SQL DEMO - Loading and Querying Tables")
print("=" * 80)

# ============================================================================
# 1. LOAD CSV DATA INTO DATAFRAMES
# ============================================================================
print("\n[1] Loading sample data from CSV files...")

customers_df = spark.read.csv(
    os.path.join(data_dir, "customers.csv"),
    header=True,
    inferSchema=True
)

products_df = spark.read.csv(
    os.path.join(data_dir, "products.csv"),
    header=True,
    inferSchema=True
)

orders_df = spark.read.csv(
    os.path.join(data_dir, "orders.csv"),
    header=True,
    inferSchema=True
)

print("✓ Data loaded successfully!")

# ============================================================================
# 2. DISPLAY DATAFRAMES
# ============================================================================
print("\n[2] Sample Data Tables")
print("\n--- CUSTOMERS TABLE ---")
customers_df.show()

print("\n--- PRODUCTS TABLE ---")
products_df.show()

print("\n--- ORDERS TABLE ---")
orders_df.show()

# ============================================================================
# 3. REGISTER DATAFRAMES AS SQL VIEWS
# ============================================================================
print("\n[3] Registering DataFrames as SQL views...")

customers_df.createOrReplaceTempView("customers")
products_df.createOrReplaceTempView("products")
orders_df.createOrReplaceTempView("orders")

print("✓ Views created: customers, products, orders")

# ============================================================================
# 4. SQL QUERIES - JOINS
# ============================================================================
print("\n[4] SQL QUERY: Customer Orders with Product Details (JOIN)")
print("-" * 80)

query_join = """
SELECT 
    c.customer_id,
    c.customer_name,
    o.order_id,
    p.product_name,
    o.quantity,
    p.price,
    o.total_amount,
    o.order_date
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
ORDER BY o.order_date DESC
"""

join_result = spark.sql(query_join)
join_result.show(20, truncate=False)

# ============================================================================
# 5. SQL QUERIES - AGGREGATIONS
# ============================================================================
print("\n[5] SQL QUERY: Total Sales by Customer (AGGREGATION)")
print("-" * 80)

query_aggregation = """
SELECT 
    c.customer_id,
    c.customer_name,
    COUNT(o.order_id) AS order_count,
    SUM(o.total_amount) AS total_spent,
    AVG(o.total_amount) AS avg_order_value,
    MAX(o.total_amount) AS highest_order
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY total_spent DESC
"""

agg_result = spark.sql(query_aggregation)
agg_result.show(truncate=False)

# ============================================================================
# 6. SQL QUERIES - FILTERING
# ============================================================================
print("\n[6] SQL QUERY: High-Value Orders (> $300)")
print("-" * 80)

query_filter = """
SELECT 
    o.order_id,
    c.customer_name,
    p.product_name,
    o.total_amount,
    o.order_date
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
WHERE o.total_amount > 300
ORDER BY o.total_amount DESC
"""

filter_result = spark.sql(query_filter)
filter_result.show(truncate=False)

# ============================================================================
# 7. SQL QUERIES - WINDOW FUNCTIONS & GROUPING
# ============================================================================
print("\n[7] SQL QUERY: Sales by Product Category")
print("-" * 80)

query_category = """
SELECT 
    p.category,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC
"""

category_result = spark.sql(query_category)
category_result.show(truncate=False)

# ============================================================================
# 8. SQL QUERIES - TOP CUSTOMERS
# ============================================================================
print("\n[8] SQL QUERY: Top 3 Customers by Revenue")
print("-" * 80)

query_top = """
SELECT 
    c.customer_id,
    c.customer_name,
    c.city,
    c.country,
    COUNT(o.order_id) AS orders,
    SUM(o.total_amount) AS total_revenue
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name, c.city, c.country
ORDER BY total_revenue DESC
LIMIT 3
"""

top_result = spark.sql(query_top)
top_result.show(truncate=False)

# ============================================================================
# 9. SUMMARY STATISTICS
# ============================================================================
print("\n[9] SQL QUERY: Summary Statistics")
print("-" * 80)

query_stats = """
SELECT 
    COUNT(DISTINCT c.customer_id) AS total_customers,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT p.product_id) AS total_products,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value,
    MIN(o.total_amount) AS min_order_value,
    MAX(o.total_amount) AS max_order_value
FROM customers c
CROSS JOIN orders o
CROSS JOIN products p
"""

stats_result = spark.sql(query_stats)
stats_result.show(truncate=False)

# ============================================================================
# 10. SAVE RESULTS (Optional)
# ============================================================================
print("\n[10] Saving query results...")

# Save join result as Parquet
join_result.write.mode("overwrite").parquet("output/customer_orders")
print("✓ Customer orders saved to: output/customer_orders")

# Save aggregation result as CSV
agg_result.write.mode("overwrite").option("header", "true").csv("output/sales_by_customer")
print("✓ Sales summary saved to: output/sales_by_customer")

# ============================================================================
# COMPLETION
# ============================================================================
print("\n" + "=" * 80)
print("✓ SPARK SQL DEMO COMPLETED SUCCESSFULLY!")
print("=" * 80)
print("\nKey takeaways:")
print("  • Loaded CSV data into Spark DataFrames")
print("  • Registered DataFrames as SQL views")
print("  • Executed JOIN queries across multiple tables")
print("  • Performed AGGREGATION queries (GROUP BY, SUM, COUNT, AVG)")
print("  • Filtered data with WHERE clauses")
print("  • Saved results to Parquet and CSV formats")
print("=" * 80)

# Stop Spark Session
spark.stop()
