"""
Given a time series DataFrame df with columns timestamp and value, calculate the moving average over a 30-day window.
"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("MovingAverage").getOrCreate()

# Sample data
data = [
    ("2024-01-01", 100),
    ("2024-01-02", 120),
    ("2024-01-03", 130),
    ("2024-01-04", 110),
    ("2024-01-05", 140)
]

df = spark.createDataFrame(data, ["timestamp", "value"])

# Convert timestamp to date type
df = df.withColumn("timestamp", col("timestamp").cast("date"))

# Define window specification for moving average
# Use rowsBetween for a fixed number of rows around the current row
window_spec = Window.orderBy("timestamp").rowsBetween(-1, 0)  # Adjust window size as needed

# Calculate moving average
df_moving_avg = df.withColumn("moving_avg", avg("value").over(window_spec))

df_moving_avg.show()
