import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, regexp_replace, trim

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
df = df.select(col("Name"), col("City"), col("Cuisine Style"))

df = df.withColumn("Cuisine Style", regexp_replace("Cuisine Style", "\\[", ""))\
    .withColumn("Cuisine Style", regexp_replace("Cuisine Style", "\\]", ""))\
    .withColumn("Cuisine Style", split(col("Cuisine Style"), ", "))\
    .withColumn("Cuisine Style", explode("Cuisine Style"))\
    .withColumn("Cuisine Style", regexp_replace("Cuisine Style", "'", ""))\
    .withColumn("Cuisine Style", trim(col("Cuisine Style")))\
    .withColumn("Cuisine", col("Cuisine Style"))\
    .drop("Cuisine Style")
df.show()
