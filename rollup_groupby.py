"""
Given a DataFrame df with columns category, sub_category, and amount, calculate the total amount for each category, each sub_category, and the grand total using groupBy and rollup.
+--------+------------+------+
|category|sub_category|amount|
+--------+------------+------+
|       A|          A1|   100|
|       A|          A2|   150|
|       B|          B1|   200|
|       B|          B2|   250|
+--------+------------+------+

Output:
+--------+------------+------------+
|category|sub_Category|total_amount|
+--------+------------+------------+
|       A|          A1|         100|
|       A|        NULL|         250|
|    NULL|        NULL|         700|
|       A|          A2|         150|
|       B|        NULL|         450|
|       B|          B1|         200|
|       B|          B2|         250|
+--------+------------+------------+
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("Complex Aggregation").getOrCreate()

df = spark.createDataFrame([
    ("A", "A1", 100),
    ("A", "A2", 150),
    ("B", "B1", 200),
    ("B", "B2", 250)
], ["category", "sub_category", "amount"])

agg_df = df.rollup("category", "sub_category").agg(sum("amount").alias("total_amount"))

agg_df.show()
