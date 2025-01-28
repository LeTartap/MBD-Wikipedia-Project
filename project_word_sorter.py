from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, split

# Initialize Spark session
spark = SparkSession.builder.appName("ReadHighestCommonWordCount").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Path to the directory containing output JSON files
hdfs_output_dir = "/user/s2539829/SHARED_MBD/rev_data/output"

# Read all JSON files in the output directory
df = spark.read.json(f"{hdfs_output_dir}/*.json")

# Ensure the schema includes the required columns
df.printSchema()

# Add a column for word count in the `text` field
df = df.withColumn("text_word_count", size(split(col("full_text"), "\\s+")))

# Find the row with the highest `common_word_count`
max_row = df.orderBy(col("common_word_count").desc()).limit(1)

# Show the result, truncating the `full_text` to 100 characters and displaying the word count
print("Row with the highest common word count:")
max_row.select(
    col("to_id"),
    col("timestamp"),
    col("common_word_count"),
    col("text_word_count"),
    col("full_text").substr(1, 100).alias("truncated_text")
).show(truncate=False)

# Stop the Spark session
spark.stop()
