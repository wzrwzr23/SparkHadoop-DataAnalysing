import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# you may add more import if you need to

# Develop a Spark application that cleans up the CSV file by removing rows
# with no reviews or rating < 1.0. Write the output as CSV into HDFS path
# /assignment2/output/question1/.
# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

df = df.filter(col("Reviews")!="[[], []]").filter(col("Rating")>=1.0)
df.show()

df.write.csv("hdfs://%s:9000/assignment2/output/question1/" % (hdfs_nn), header=True)

