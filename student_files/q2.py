import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

df = df.na.drop(subset=["Price Range"])

best = df.groupBy(["Price Range", "City"]).agg(max("Rating")).withColumn("Rating", col("max(Rating)")).drop("max(Rating)")
worst = df.groupBy(["Price Range", "City"]).agg(min("Rating")).withColumn("Rating", col("min(Rating)")).drop("min(Rating)")
# best.show()
# worst.show()
unioned = best.union(worst)
joined = unioned.join(df, on=["Price Range", "City", "Rating"], how="inner")
joined = joined.orderBy("City").dropDuplicates(["Price Range", "City", "Rating"])
joined.show()


joined.write.csv("hdfs://%s:9000/assignment2/output/question2/" % (hdfs_nn), header=True)
