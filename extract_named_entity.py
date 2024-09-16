"""
You need to process a DataFrame df_text with a column content containing unstructured text.
Implement a solution to extract named entities (e.g., people, organizations) using an external text analysis library.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import spacy

spark = SparkSession.builder.appName("TextAnalysis").getOrCreate()

nlp = spacy.load("en_core_web_sm")

df_text = spark.createDataFrame([
    ("Apple is looking at buying U.K. startup for $1 billion.",),
    ("Barack Obama was the 44th President of the United States.",)
], ["content"])

def extract_entities(text):
    doc = nlp(text)
    return ", ".join([ent.text for ent in doc.ents])

udf_extract_entities = udf(extract_entities, StringType())

df_entities = df_text.withColumn("entities", udf_extract_entities(col("content")))

df_entities.show()