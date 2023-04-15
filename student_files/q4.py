import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, arrays_zip, explode, regexp_replace

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
df = df.select(col("Name"), col("City"), col("Cuisine Style"))
df.show()
