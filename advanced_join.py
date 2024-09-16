"""
You have two DataFrames: df_orders with columns order_id, customer_id, and order_amount, and df_customers
with columns customer_id, customer_name, and customer_segment. df_orders has 10 million rows, and df_customers has 500,000 rows.
Perform an efficient join to get the total order amount per customer segment and explain your strategy for handling the data size.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("AdvancedJoinStrategy").getOrCreate()

# Load data
df_orders = spark.read.csv("./input_data/df_orders.csv", header=True, inferSchema=True)
df_customers = spark.read.csv("./input_data/df_customers.csv", header=True, inferSchema=True)

# Broadcast the smaller DataFrame to optimize the join
from pyspark.sql.functions import broadcast

joined_df = df_orders.join(broadcast(df_customers), on="customer_id")

# Aggregate to get total order amount per customer segment
result_df = joined_df.groupBy("customer_segment").agg(sum("order_amount").alias("total_order_amount"))

result_df.show()
