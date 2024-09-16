"""
You have a large DataFrame df with a date column. How would you efficiently read and process data for a specific date range by partitioning the data?
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("PartitionedRead").getOrCreate()

# Load data with partitioning
df = spark.read.parquet("path_to_partitioned_data")

# Filter data for a specific date range
filtered_df = df.filter((col("date") >= "2024-01-01") & (col("date") <= "2024-01-31"))

filtered_df.show()
