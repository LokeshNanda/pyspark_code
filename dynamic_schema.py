"""
You have a streaming DataFrame df_stream with evolving schema (i.e., new columns may appear). How would you handle schema evolution in PySpark?
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("SchemaEvolution").getOrCreate()

# Load streaming data with evolving schema
df_stream = spark.readStream.option("mergeSchema", "true").json("streaming_data")

# Process data dynamically
df_processed = df_stream.select(*[col(c) for c in df_stream.columns])

query = df_processed.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
