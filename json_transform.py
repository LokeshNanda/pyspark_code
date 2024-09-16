"""
You have a DataFrame df with a column json_data containing nested JSON strings.
How would you extract a nested field user.name and user.email from the JSON?
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object

spark = SparkSession.builder.appName("NestedJSON").getOrCreate()

# Sample data
df = spark.createDataFrame([
    ('{"user": {"name": "John Doe", "email": "john.doe@example.com"}}',),
    ('{"user": {"name": "Jane Smith", "email": "jane.smith@example.com"}}',)
], ["json_data"])

# Extract nested fields
df_extracted = df.select(
    get_json_object(col("json_data"), "$.user.name").alias("name"),
    get_json_object(col("json_data"), "$.user.email").alias("email")
)

df_extracted.show()

