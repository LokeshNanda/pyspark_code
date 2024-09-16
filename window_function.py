"""
 Given a DataFrame of employee data with columns employee_id, department, and salary,
 compute the rank of each employee within their department based on salary, where the highest salary gets rank 1.

 +-----------+----------+------+
|employee_id|department|salary|
+-----------+----------+------+
|          1|        HR| 50000|
|          2|        HR| 60000|
|          3|        IT| 70000|
|          4|        IT| 80000|
|          5|        IT| 75000|
+-----------+----------+------+

Output:
+-----------+----------+------+----+
|employee_id|department|salary|Rank|
+-----------+----------+------+----+
|          2|        HR| 60000|   1|
|          1|        HR| 50000|   2|
|          4|        IT| 80000|   1|
|          5|        IT| 75000|   2|
|          3|        IT| 70000|   3|
+-----------+----------+------+----+
"""
from pyspark.sql.window import Window
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import rank

spark = SparkSession.builder.appName("Window Functions").getOrCreate()

data = [
    (1, "HR", 50000),
    (2, "HR", 60000),
    (3, "IT", 70000),
    (4, "IT", 80000),
    (5, "IT", 75000)
]

df = spark.createDataFrame(data, schema=["employee_id", "department", "salary"])
windowSpec = Window.partitionBy("department").orderBy(df["salary"].desc())

df_with_rank = df.withColumn("Rank", rank().over(windowSpec))

df_with_rank.show()

