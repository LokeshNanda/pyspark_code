# Write a PySpark SQL query to find the top 2 products by sales volume in each category.

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TopProductsByCategory").getOrCreate()

# Sample data
df = spark.createDataFrame([
    ("Electronics", "Laptop", 3000),
    ("Electronics", "Smartphone", 5000),
    ("Furniture", "Chair", 1500),
    ("Furniture", "Table", 2000),
    ("Electronics", "Tablet", 2500),
    ("Furniture", "Sofa", 3500),
    ("Clothing", "Jeans", 22000),
    ("Clothing", "Tshirt", 55000),
    ("Clothing", "Hat", 500),
    ("Clothing", "Gloves", 100)
], ["category", "product", "sales"])

# Register DataFrame as SQL temp view
df.createOrReplaceTempView("sales_data")

# SQL query to find top 2 products by sales volume in each category
query = """
SELECT category, product, sales
FROM (
    SELECT category, product, sales,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as rank
    FROM sales_data
)
WHERE rank <= 2
"""

top_products_df = spark.sql(query)
top_products_df.show()
