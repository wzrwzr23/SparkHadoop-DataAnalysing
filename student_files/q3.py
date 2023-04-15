import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min, split, count

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

rev = split(df["Reviews"], '\\], \\[')
rev_df = df.withColumn("review", rev.getItem(0).withColumn("date", rev.getItem(1)))
rev_df.show()