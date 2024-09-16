"""
Create a User-Defined Function (UDF) that takes a string column and returns the length of the string.
Apply this UDF to a DataFrame with a column text.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("UDFExample").getOrCreate()

data = [("Hello",), ("Spark",), ("Data",)]
df = spark.createDataFrame(data, ["text"])

def string_length(s):
    return len(s)

length_udf = udf(string_length, IntegerType())
df_with_length = df.withColumn("text_length", length_udf(df["text"]))

df_with_length.show()
