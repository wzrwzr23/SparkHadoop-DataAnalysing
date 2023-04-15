import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min, count

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

df = df.na.drop(subset=["Price Range"])

grp = ["Price Range", "City"]
best = df.groupBy(grp).agg(max("Rating")).withColumn("Rating", col("max(Rating)")).drop("max(Rating)")
worst = df.groupBy(grp).agg(min("Rating")).withColumn("Rating", col("min(Rating)")).drop("min(Rating)")

print("Best: ", best.count(), " Worst: ", worst.count())
combined = best.union(worst)
print("Combined: ", combined.count())
combined.show()



df.write.csv("hdfs://%s:9000/assignment2/output/question1/" % (hdfs_nn), header=True)
df.show()
