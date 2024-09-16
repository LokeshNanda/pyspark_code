from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging

# Create a SparkConf with custom configurations
# conf = SparkConf() \
#     .set("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/C:/Users/Lokesh%20Nanda/PycharmProjects/pyspark_practice/log4j.properties") \
#     .set("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:/C:/Users/Lokesh%20Nanda/PycharmProjects/pyspark_practice/log4j.properties")

# Initialize SparkSession with the SparkConf
spark = SparkSession.builder \
    .appName("PySpark Example") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
# Create a DataFrame
data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
df = spark.createDataFrame(data, ["Name", "Id"])

# Show the DataFrame
df.show()

# Stop the SparkSession
spark.stop()
