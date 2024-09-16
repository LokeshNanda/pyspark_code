"""
Skewed Data with Salting
Imagine you're working with a large e-commerce platform. You need to join two massive datasets:

Order Data (df_large1): Contains millions of records detailing customer orders.
Customer Data (df_large2): Contains customer information with hundreds of thousands of records.
The customer_id is used as the join key, but data is highly skewed: some customers (e.g., large enterprise accounts or resellers) have a disproportionately high number of orders, creating performance bottlenecks when performing the join on customer_id.

Steps to Optimize Join with Salting and Explanation
Understand the Skew:

Some customer_ids appear thousands of times in the order dataset, while others appear only a few times.
Direct joins on the customer_id lead to an uneven distribution of data across partitions, with certain partitions becoming overloaded, causing performance issues.
Salting Strategy:

To handle this, we’ll add a "salt" to the customer_id column, creating composite keys to distribute the load evenly across partitions.

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, hash

# Initialize Spark session
spark = SparkSession.builder.appName("DataSkewMitigation").getOrCreate()

# Load sample data
df_large1 = spark.read.csv("./input_Data/large_data1.csv", header=True, inferSchema=True)  # Orders data
df_large2 = spark.read.csv("./input_data/large_data2.csv", header=True, inferSchema=True)  # Customer data

# Step 1: Add a salt column to the skewed key (customer_id in this case)
salt_mod = 10  # Number of salt partitions; adjust based on data skew severity
df_large1_salted = df_large1.withColumn("salt", (hash(col("customer_id")) % salt_mod).cast("string"))
df_large2_salted = df_large2.withColumn("salt", (hash(col("customer_id")) % salt_mod).cast("string"))

# Step 2: Create composite keys by combining customer_id with the salt column
df_large1_salted = df_large1_salted.withColumn("composite_key", concat_ws("_", col("customer_id"), col("salt")))
df_large2_salted = df_large2_salted.withColumn("composite_key", concat_ws("_", col("customer_id"), col("salt")))

# Step 3: Perform the join using the composite key to mitigate skew
result_df = df_large1_salted.join(df_large2_salted, on="composite_key")

# Step 4: View results
result_df.show()

# Continue processing (e.g., aggregating order amounts by customer segment, etc.)