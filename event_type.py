"""
 Given a DataFrame df with columns timestamp, user_id, and event_type,
 calculate the number of unique users for each event_type in hourly time intervals.

 Output:
 +------------------------------------------+----------+------------+
|window                                    |event_type|unique_users|
+------------------------------------------+----------+------------+
|{2024-08-31 23:30:00, 2024-09-01 00:30:00}|click     |1           |
|{2024-09-01 01:30:00, 2024-09-01 02:30:00}|view      |2           |
|{2024-09-01 01:30:00, 2024-09-01 02:30:00}|click     |1           |
+------------------------------------------+----------+------------+
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, countDistinct

spark = SparkSession.builder.appName("AdvancedDataframeOperations").getOrCreate()

# Sample data
data = [
    ("2024-09-01 00:15:00", 1, "click"),
    ("2024-09-01 01:45:00", 2, "view"),
    ("2024-09-01 01:30:00", 1, "click"),
    ("2024-09-01 02:15:00", 3, "view")
]

df = spark.createDataFrame(data, ["timestamp", "user_id", "event_type"])

# Convert timestamp to timestamp type
df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Calculate unique users for each event_type in hourly intervals
df_with_window = df.groupBy(window("timestamp", "1 hour"), "event_type").agg(countDistinct("user_id").alias("unique_users"))

df_with_window.show(truncate=False)
