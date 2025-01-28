from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, collect_list, lower, regexp_replace, size, split
from pyspark.sql.types import IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("CountUncommonWords").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Path to the HDFS directory containing input JSON files
hdfs_directory = "/user/s2539829/SHARED_MBD/rev_data"

# Path to the CSV file containing common ChatGPT words
common_words_csv = "combined_chatgpt_words.csv"

# Load the list of common ChatGPT words using Spark
common_words_df = spark.read.csv(common_words_csv, header=False).toDF("word")
common_words = set(row["word"].lower() for row in common_words_df.collect())

# Broadcast the common words set to all Spark workers
common_words_broadcast = spark.sparkContext.broadcast(common_words)

# Define a function to count uncommon words
def count_uncommon_words(text):
    if not text:
        return 0
    words = text.split()
    uncommon_count = sum(1 for word in words if word not in common_words_broadcast.value)
    return uncommon_count

# Register the function as a UDF
count_uncommon_words_udf = spark.udf.register("count_uncommon_words", count_uncommon_words, IntegerType())

# Dynamically list all relevant titles in the directory
all_files = [f.path for f in spark.read.format("binaryFile").load(f"{hdfs_directory}/*_revisions.json").select("path").collect()]

def get_title_from_path(file_path):
    return file_path.split("/")[-1].replace("_revisions.json", "")

titles = [get_title_from_path(file) for file in all_files]

# Process each file group
for title in titles:
    metadata_path = f"{hdfs_directory}/{title}_revisions.json"
    content_path = f"{hdfs_directory}/{title}_revision_content.json"

    # Load metadata and content
    metadata_df = spark.read.json(metadata_path)
    content_df = spark.read.json(content_path)

    # Join on `to_id` and group text by `to_id`
    joined_df = content_df.join(metadata_df, content_df["to_id"] == metadata_df["id"], "inner")
    grouped_texts = joined_df.groupBy("to_id", "timestamp").agg(
        concat_ws(" ", collect_list("text")).alias("full_text")
    )

    # Clean and tokenize text, then count uncommon words
    grouped_texts = grouped_texts.withColumn(
        "clean_text",
        regexp_replace(lower(col("full_text")), "[^a-zA-Z\s]", "")
    )
    grouped_texts = grouped_texts.withColumn(
        "uncommon_word_count", count_uncommon_words_udf(col("clean_text"))
    )

    # Select relevant columns
    result_df = grouped_texts.select(
        col("to_id").alias("revision_id"),
        col("timestamp"),
        col("uncommon_word_count")
    )

    # Save results to HDFS
    output_path = f"{hdfs_directory}/outputUncommon/{title}_uncommon_words.json"
    result_df.write.json(output_path, mode="overwrite")
    print(f"Saved results to {output_path}")

spark.stop()
