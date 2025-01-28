from pyspark.sql import SparkSession
import subprocess
import re
from pyspark.sql.functions import udf, col, concat_ws, collect_list, explode, split, lower, count, sum
from pyspark.sql.types import StringType, IntegerType

# Function to list files in HDFS
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

# HDFS directory containing the data
hdfs_directory = "/user/s2539829/SHARED_MBD/rev_data"

# List all files in the HDFS directory
all_files = list_hdfs_files(hdfs_directory)

# Group files by title using regex
def get_title(filename):
    match = re.match(r".*/(.*)_revision(s|_content).json", filename)
    return match.group(1) if match else None

file_groups = {}
for file in all_files:
    title = get_title(file)
    if title:
        if title not in file_groups:
            file_groups[title] = {"metadata": None, "content": None}
        if "revisions.json" in file:
            file_groups[title]["metadata"] = file
        elif "revision_content.json" in file:
            file_groups[title]["content"] = file

# Filter out incomplete groups
file_groups = {title: paths for title, paths in file_groups.items() if paths["metadata"] and paths["content"]}

# Initialize Spark session
spark = SparkSession.builder.appName("RevisionProcessing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# DataFrame to hold aggregated word counts across all revisions
all_word_counts_df = None

for title, paths in file_groups.items():
    print(f"Processing: {title}")

    # Load metadata and content from HDFS
    metadata_df = spark.read.json(paths["metadata"])
    content_df = spark.read.json(paths["content"])

    # Join on `to_id`
    joined_df = content_df.join(metadata_df, content_df["to_id"] == metadata_df["id"], "inner")

    # Combine all text into a single document per revision
    grouped_texts = joined_df.groupBy("to_id").agg(
        concat_ws(" ", collect_list("text")).alias("full_text")
    )

    # Tokenize words, convert to lowercase, and count occurrences
    word_counts = grouped_texts.select(
        explode(split(lower(col("full_text")), "\\s+")).alias("word")
    ).groupBy("word").agg(count("word").alias("count"))

    # Save word counts for this article to HDFS
    output_path = f"/user/s2539829/SHARED_MBD/rev_data/output/{title}_word_counts.json"
    word_counts.write.json(output_path, mode="overwrite")
    print(f"Saved results to {output_path}")

    # Aggregate word counts across all revisions
    if all_word_counts_df is None:
        all_word_counts_df = word_counts
    else:
        all_word_counts_df = all_word_counts_df.union(word_counts).groupBy("word").agg(
            sum("count").alias("count")
        )

# Sort the aggregated word counts by count in descending order
if all_word_counts_df is not None:
    all_word_counts_sorted_df = all_word_counts_df.orderBy(col("count").desc())

    # Save aggregated and sorted word counts to HDFS
    aggregated_output_path = "/user/s2539829/SHARED_MBD/rev_data/output/all_word_counts_sorted.json"
    all_word_counts_sorted_df.write.json(aggregated_output_path, mode="overwrite")
    print(f"Saved aggregated and sorted word counts to {aggregated_output_path}")

spark.stop()
