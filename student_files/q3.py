import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, arrays_zip, explode, regexp_replace, trim

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

df = df.select(df.ID_TA, df.Reviews)

reviews = split(df.Reviews, '\\], \\[')
rev_df = df.withColumn("rev", reviews.getItem(0))\
        .withColumn("dat", reviews.getItem(1))\
        .withColumn("review", split(col("rev"), "\\', \\'"))\
        .withColumn("date", split(col("dat"), "\\', \\'"))\
        .drop(col("rev")).drop(col("dat"))\
        .withColumn("combined", arrays_zip("review", "date"))\
        .withColumn("combined", explode("combined"))\
        .withColumn("review", col("combined.review"))\
        .withColumn("date", col("combined.date"))\
        .drop(col("Reviews")).drop(col("combined"))\
        .withColumn("review", regexp_replace("review", "\\[", ""))\
        .withColumn("date", regexp_replace("date", "\\]", ""))\
        .withColumn("review", regexp_replace("review", "'", ""))\
        .withColumn("date", regexp_replace("date", "'", ""))\
        .withColumn("review", trim(col("review")))\
        .withColumn("date", trim(col("date")))
rev_df.show()
rev_df.write.csv("hdfs://%s:9000/assignment2/output/question3/" % (hdfs_nn), header=True)