"""
You have two large DataFrames, df1 and df2.
Perform a join between these DataFrames on the id column and optimize the join operation for performance.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("OptimizeJoins").getOrCreate()

# Create example DataFrames
df1 = spark.read.csv("./input_data/df1.csv", header=True, inferSchema=True)
df2 = spark.read.csv("./input_data/df2.csv", header=True, inferSchema=True)

# Assume df2 is smaller
optimized_join_df = df1.join(broadcast(df2), df1["id"] == df2["id"])

# filtered_df = optimized_join_df.where(df1["id"] == 1)
# filtered_df.show()
optimized_join_df.show()