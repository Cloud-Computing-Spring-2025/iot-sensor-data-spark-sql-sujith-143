from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, avg, hour
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
from pyspark.sql.functions import desc

# Initialize Spark Session
spark = SparkSession.builder.appName("IoT Sensor Data").getOrCreate()

# Load the CSV
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

# Task 1: Load & Basic Exploration
# Show the first 5 rows
df.show(5)

# Count the total number of records
record_count = df.count()
print(f"Total number of records: {record_count}")

# Retrieve the distinct set of locations
df.select("location").distinct().show()

# Create a Temporary View
df.createOrReplaceTempView("sensor_readings")

# Save Task 1 Output
df.write.csv("task1_output.csv", header=True)

# Task 2: Filtering & Simple Aggregations
# Filter rows where temperature is between 18 and 30
filtered_df = df.filter((df.temperature >= 18) & (df.temperature <= 30))

# Count rows in-range vs. out-of-range
out_of_range_count = df.count() - filtered_df.count()
in_range_count = filtered_df.count()
print(f"Out of range: {out_of_range_count}, In range: {in_range_count}")

# Aggregation: Group by location and compute the average temperature and humidity
agg_df = filtered_df.groupBy("location").agg(
    avg("temperature").alias("avg_temperature"),
    avg("humidity").alias("avg_humidity")
).orderBy("avg_temperature", ascending=False)

agg_df.show()

# Save Task 2 Output
agg_df.write.csv("task2_output.csv", header=True)

# Task 3: Time-Based Analysis
# Convert timestamp string to proper timestamp format
df = df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
df.createOrReplaceTempView("sensor_readings")

# Group by Hour: Extract the hour of the day and compute the average temperature
hourly_df = spark.sql("""
    SELECT HOUR(timestamp) AS hour_of_day, AVG(temperature) AS avg_temp
    FROM sensor_readings
    GROUP BY hour_of_day
    ORDER BY hour_of_day
""")

hourly_df.show()

# Save Task 3 Output
hourly_df.write.csv("task3_output.csv", header=True)

# Rank Sensors by Average Temperature
# Compute average temperature per sensor
ranked_df = df.groupBy("sensor_id").agg(avg("temperature").alias("avg_temp"))

# Define a window specification to rank the sensors by avg_temp (descending)
window_spec = Window.orderBy(desc("avg_temp"))

# Rank the sensors based on their average temperature
ranked_df = ranked_df.withColumn("rank_temp", rank().over(window_spec))

# Show the top 5 sensors with their rank
ranked_df.show(5)

# Save Task 4 Output
ranked_df.write.csv("task4_output.csv", header=True)


# Task 5: Pivot & Interpretation
# Pivot by Hour and Location: Compute the average temperature
pivot_df = df.groupBy("location", hour("timestamp").alias("hour_of_day")) \
             .agg(avg("temperature").alias("avg_temperature"))

pivot_df = pivot_df.groupBy("location").pivot("hour_of_day", range(24)) \
                   .agg(avg("avg_temperature"))

pivot_df.show()

# Save Task 5 Output
pivot_df.write.csv("task5_output.csv", header=True)

# Stop Spark Session
spark.stop()
