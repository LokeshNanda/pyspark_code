"""
Calculate the cumulative sum of sales for each store_id ordered by date using window functions.

"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("CumulativeSum").getOrCreate()

# Sample data
df = spark.createDataFrame([
    ("2024-01-01", "Store1", 100),
    ("2024-01-02", "Store1", 150),
    ("2024-01-03", "Store1", 300),
    ("2024-01-01", "Store2", 200),
    ("2024-01-02", "Store2", 250)
], ["date", "store_id", "sales"])

# Define window specification
window_spec = Window.partitionBy("store_id").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate cumulative sum
df_cumulative_sum = df.withColumn("cumulative_sales", sum("sales").over(window_spec))

df_cumulative_sum.show()
