"""
Given a DataFrame with columns date, product, and sales, calculate the rolling 7-day average sales for each product.
"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, window

spark = SparkSession.builder.appName("RollingAverage").getOrCreate()

data = [
    ("2024-09-01", "Laptop", 1200),
    ("2024-09-02", "Laptop", 1300),
    ("2024-09-03", "Laptop", 1100),
    ("2024-09-04", "Laptop", 1400),
    ("2024-09-05", "Laptop", 1250),
    ("2024-09-06", "Laptop", 1500),
    ("2024-09-07", "Laptop", 1600),
    ("2024-09-08", "Laptop", 1700),

    ("2024-09-01", "Smartphone", 800),
    ("2024-09-02", "Smartphone", 850),
    ("2024-09-03", "Smartphone", 750),
    ("2024-09-04", "Smartphone", 900),
    ("2024-09-05", "Smartphone", 950),
    ("2024-09-06", "Smartphone", 1000),
    ("2024-09-07", "Smartphone", 1100),
    ("2024-09-08", "Smartphone", 1150),

    ("2024-09-01", "Jeans", 400),
    ("2024-09-02", "Jeans", 450),
    ("2024-09-03", "Jeans", 350),
    ("2024-09-04", "Jeans", 500),
    ("2024-09-05", "Jeans", 550),
    ("2024-09-06", "Jeans", 600),
    ("2024-09-07", "Jeans", 650),
    ("2024-09-08", "Jeans", 700),

    ("2024-09-01", "Bread", 50),
    ("2024-09-02", "Bread", 60),
    ("2024-09-03", "Bread", 55),
    ("2024-09-04", "Bread", 65),
    ("2024-09-05", "Bread", 70),
    ("2024-09-06", "Bread", 75),
    ("2024-09-07", "Bread", 80),
    ("2024-09-08", "Bread", 85)
]

df = spark.createDataFrame(data, ["date", "product", "sales"])

windowSpec = Window.partitionBy("product").orderBy("date").rowsBetween(-6, 0)

df_with_rolling_avg = df.withColumn("rolling_avg", avg("sales").over(windowSpec))

df_with_rolling_avg.show()