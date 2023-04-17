import sys 
from pyspark.sql import SparkSession
# you may add more import if you need to
import pyspark.sql.types as T
from pyspark.sql.functions import explode, from_json, col, array, array_sort, count

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header",True)\
    .parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))

df = df.drop(df.crew)

castSchema = T.ArrayType(T.StructType([T.StructField("name", T.StringType(), False)]))

# df_bk = spark.createDataFrame(data=arrayData, schema =["movie_id","title","actor1","actor2"])
df = df.withColumn("actor1", explode(from_json(col("cast"), castSchema).getField("name")))\
    .withColumn("actor2", explode(from_json(col("cast"), castSchema).getField("name")))\
    .drop(col("cast"))\
    .filter(col("actor1")!=col("actor2"))\
    .withColumn("pair", array(col("actor1"), col("actor2")))\
    .withColumn("pair", array_sort(col("pair")).cast("string"))\
    .dropDuplicates(["movie_id", "title", "pair"]).orderBy("pair")
num = df.groupBy(col("pair"))\
    .agg(count("*"))\
    .filter(col("count(1)")>=2)\
    .orderBy("pair")
actor_pair = num.join(df, ["pair"], "inner")\
    .drop("pair", "count(1)")\
    .orderBy("movie_id")
actor_pair.show()
actor_pair.write.option("header", True).mode("overwrite").parquet("hdfs://%s:9000/assignment2/output/question5/" % (hdfs_nn))