from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, concat_ws, collect_list, split, explode, lower, count
from pyspark.sql.types import StringType, IntegerType
import subprocess
import re

# Initialize Spark session
spark = SparkSession.builder.appName("DynamicRevisionProcessing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# HDFS base directory
hdfs_base_dir = "/user/s2539829/SHARED_MBD/rev_data"

# Function to list files in an HDFS directory
def list_hdfs_files(hdfs_path):
    try:
        output = subprocess.check_output(["hdfs", "dfs", "-ls", hdfs_path], universal_newlines=True)
        files = []
        for line in output.split("\n"):
            parts = line.split()
            if len(parts) > 0 and parts[-1].startswith(hdfs_path):
                files.append(parts[-1])
        return files
    except subprocess.CalledProcessError as e:
        print(f"Error listing HDFS files: {e}")
        return []

# List all files in the base directory
all_files = list_hdfs_files(hdfs_base_dir)

# Function to group files by title
def group_files_by_title(files):
    grouped = {}
    for file in files:
        match = re.match(r".*/(.*)_(revision_content|revisions).json", file)
        if match:
            title = match.group(1)
            file_type = match.group(2)
            if title not in grouped:
                grouped[title] = {"content": None, "metadata": None}
            if file_type == "revision_content":
                grouped[title]["content"] = file
            elif file_type == "revisions":
                grouped[title]["metadata"] = file
    return {title: paths for title, paths in grouped.items() if paths["content"] and paths["metadata"]}

# Group files by title
file_groups = group_files_by_title(all_files)

# Define a UDF to count "common" words
common_words = ["explore", "leverage", "resonate"]  # Example placeholder
def count_common_words(text):
    words = text.split()
    return sum(1 for word in words if word.lower() in common_words)

common_words_udf = udf(count_common_words, IntegerType())

# Process each group
for title, paths in file_groups.items():
    print(f"Processing: {title}")

    # Load metadata and content files
    metadata_df = spark.read.json(paths["metadata"])
    content_df = spark.read.json(paths["content"])

    # Join on `to_id`
    joined_df = content_df.join(metadata_df, content_df["to_id"] == metadata_df["id"], "inner")

    # Combine text by revision and count common words
    processed_df = joined_df.groupBy("to_id", "timestamp").agg(
        concat_ws(" ", collect_list("text")).alias("full_text")
    ).withColumn(
        "common_word_count", common_words_udf(col("full_text"))
    )

    # Save results for each title
    output_path = f"{hdfs_base_dir}/output/{title}_common_word_counts.json"
    processed_df.write.json(output_path, mode="overwrite")
    print(f"Saved results to {output_path}")

spark.stop()
