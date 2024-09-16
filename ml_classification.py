from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier

# Initialize SparkSession
spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

# Define the schema of the JSON data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Read data from JSON files as a stream
transactions_df = spark.readStream.schema(schema).json("./input_data/transactions")

# Feature Engineering
assembler = VectorAssembler(inputCols=["amount"], outputCol="features")
transactions_df = assembler.transform(transactions_df)

# Load or define a dummy model
# In a real scenario, you would load a pre-trained model
rf = RandomForestClassifier(labelCol="label", featuresCol="features")
model = rf.fit(transactions_df)  # Dummy fit, replace with actual model loading

# Predict fraud
predictions = model.transform(transactions_df)
flagged_transactions = predictions.filter(col("prediction") == 1)

# Write results to the console
query = flagged_transactions.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
