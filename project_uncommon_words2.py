from pyspark.sql import SparkSession
import subprocess
import re


from pyspark.sql.functions import udf, explode
from pyspark.sql.types import DoubleType, ArrayType, StringType
from pyspark.sql.functions import collect_list, col, concat_ws



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


spark = SparkSession.builder.appName("RevisionProcessing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

for title, paths in file_groups.items():
    # Load metadata and content from HDFS
    metadata_df = spark.read.json(paths["metadata"])
    content_df = spark.read.json(paths["content"])

    # Join on `to_id`
    joined_df = content_df.join(metadata_df, content_df["to_id"] == metadata_df["id"], "inner")

    # Group text by `to_id` and aggregate text
    grouped_texts = joined_df.groupBy("to_id").agg(
        collect_list("text").alias("texts")
    )
    grouped_texts.show(truncate=False)  # Check if data is grouped correctly

    # Combine all text into a single document per revision
    grouped_texts = grouped_texts.withColumn("full_text", concat_ws(" ", col("texts")))
    grouped_texts.show(truncate=False)  # Check the final result before saving

    # Save intermediate results to HDFS
    grouped_texts.write.json(f"/user/s2539829/SHARED_MBD/rev_data/output/{title}_grouped.json", mode="overwrite")
    



# Tokenize text
tokenizer = udf(lambda text: re.findall(r'\b\w+\b', text.lower()), ArrayType(StringType()))
tokenized_df = grouped_texts.withColumn("words", tokenizer(col("full_text")))

# Load a word frequency dictionary (example, replace with a real one)
word_frequencies = {"the": 0.065, "is": 0.047, "turkey": 0.0005}  # Example
bc_word_freq = spark.sparkContext.broadcast(word_frequencies)

# Calculate uncommonness score
def uncommonness_score(words):
    return sum(1 / bc_word_freq.value.get(word, 0.00001) for word in words) / len(words)

uncommonness_udf = udf(uncommonness_score, DoubleType())
scored_df = tokenized_df.withColumn("uncommon_index", uncommonness_udf(col("words")))

# Save results back to HDFS
scored_df.select("to_id", "uncommon_index").write.csv("/user/s2539829/SHARED_MBD/rev_data/output/uncommon_indices.csv", header=True, mode="overwrite")
