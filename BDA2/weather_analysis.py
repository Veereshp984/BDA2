from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Start Spark
spark = SparkSession.builder.appName("SimpleWeather").getOrCreate()

# Load CSV
df = spark.read.csv("weather.csv", header=True, inferSchema=True)

# Count each condition per day
condition_counts = df.groupBy("Date", "Condition").agg(count("*").alias("count"))

# Sort so most frequent conditions are on top
sorted_counts = condition_counts.orderBy("Date", "count", ascending=[True, False])

# Drop duplicate dates, keeping the first (most frequent)
top_conditions = sorted_counts.dropDuplicates(["Date"])

# Show result
top_conditions.select("Date", "Condition").show()

# Stop Spark
spark.stop()

