import sys
from pyspark.sql import SparkSession
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

df=spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv")
df.printSchema()