from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set

# Step 1: Start Spark
spark = SparkSession.builder.appName("MovieTags").getOrCreate()

# Step 2: Read CSV file
df = spark.read.csv("movietags.csv", header=True)

# Step 3: Select movieId and tag columns only
df_tags = df.select("movieId", "tag").dropDuplicates()

# Step 4: Group by movieId and collect all unique tags
movie_tags = df_tags.groupBy("movieId").agg(collect_set("tag").alias("tags"))

# Step 5: Show result
movie_tags.show(truncate=False)

# Step 6: Stop Spark
spark.stop()

