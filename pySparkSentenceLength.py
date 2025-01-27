from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when, length, size
from pyspark.sql.types import BooleanType, ArrayType, StringType, IntegerType
import re

# Initialize Spark session
spark = SparkSession.builder.appName("Wikipedia Sentence Analysis").getOrCreate()

# Load only _revision_content.json files
data_path = "/user/s2539829/SHARED_MBD/rev_data/*_revision_content.json"
df = spark.read.json(data_path)

# Select only relevant columns
df = df.select("date", "to_id", "text")

# Define a UDF to filter valid sentences
def is_valid_sentence(text):
    if not text:
        return False
    return bool(re.search(r'[a-zA-Z0-9]+.*[.!?]$', text))

is_valid_sentence_udf = udf(is_valid_sentence, BooleanType())

# Filter out invalid rows
df = df.filter(col("text").isNotNull() & is_valid_sentence_udf(col("text")))

# UDF to split text into sentences
def split_into_sentences(text):
    return re.split(r'(?<=[.!?]) +', text)

split_sentences_udf = udf(split_into_sentences, ArrayType(StringType()))
df = df.withColumn("sentences", split_sentences_udf(col("text")))
df.select("text", "sentences").show(truncate=False)

# Add sentence count column
df = df.withColumn("sentence_count", size(col("sentences")))

# UDF to calculate average sentence length
def avg_sentence_length(sentences):
    if not sentences:  # Handle empty or None cases
        return 0
    # Calculate word count for each sentence
    word_counts = [len(sentence.split()) for sentence in sentences]
    # Return average word count
    return sum(word_counts) / len(sentences) if len(sentences) > 0 else 0

# Convert the function into a UDF
avg_sentence_length_udf = udf(avg_sentence_length, IntegerType())

# Add columns for sentences and analysis
df = df.withColumn(
    "avg_sentence_length",
    avg_sentence_length_udf(col("sentences"))
)

# Select and show results
df.select("date", "to_id", "sentence_count", "avg_sentence_length").show(truncate=False)

# Save results to HDFS
# Uncomment the line below if you want to save to HDFS
# df.write.mode("overwrite").csv("/user/s2539829/SHARED_MBD/rev_data_sentence_analysis")
