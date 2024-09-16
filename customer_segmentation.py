from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

# Initialize SparkSession
spark = SparkSession.builder.appName("CustomerSegmentation").getOrCreate()

# Define paths to data files
transactions_path = "./input_data/transactions.csv"
customers_path = "./input_data/customers.csv"

# Load data
transactions_df = spark.read.csv(transactions_path, header=True, inferSchema=True)
customers_df = spark.read.csv(customers_path, header=True, inferSchema=True)

# Clean transaction data
transactions_df = transactions_df.dropna().dropDuplicates()

# Join transactions with customer data
customer_transactions_df = transactions_df.join(customers_df, "customer_id", "inner")

# Feature Engineering
customer_features_df = customer_transactions_df.groupBy("customer_id").agg(
    {"amount": "sum", "transaction_id": "count"}
).withColumnRenamed("sum(amount)", "total_spending") \
 .withColumnRenamed("count(transaction_id)", "transaction_count")

# Assemble features into a feature vector
assembler = VectorAssembler(inputCols=["total_spending", "transaction_count"], outputCol="features")
customer_features_df = assembler.transform(customer_features_df)

# Apply K-Means clustering
kmeans = KMeans(k=3, seed=1)  # k=3 for 3 clusters
model = kmeans.fit(customer_features_df)
clustered_df = model.transform(customer_features_df)

# Generate summaries by cluster
cluster_summary_df = clustered_df.groupBy("prediction").agg(
    {"total_spending": "avg", "transaction_count": "avg"}
).withColumnRenamed("avg(total_spending)", "avg_total_spending") \
 .withColumnRenamed("avg(transaction_count)", "avg_transaction_count")

# Save the report to a CSV file
#cluster_summary_df.write.csv("./output_data/cluster_summary.csv", header=True)

# Show the results
cluster_summary_df.show()
