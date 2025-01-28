from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, collect_list

import os
import re
from pyspark.sql import SparkSession

# Extract common title from filenames
def get_title(filename):
    match = re.match(r"(.*)_revision(s|_content).json", filename)
    return match.group(1) if match else None

# List all files in the directory
directory = "/user/s2539829/SHARED_MBD/rev_data"

files = os.listdir(directory)

# Group files by title
file_groups = {}
for file in files:
    title = get_title(file)
    if title:
        if title not in file_groups:
            file_groups[title] = {"metadata": None, "content": None}
        if "revisions.json" in file:
            file_groups[title]["metadata"] = os.path.join(directory, file)
        elif "revision_content.json" in file:
            file_groups[title]["content"] = os.path.join(directory, file)

# Filter out incomplete groups
file_groups = {title: paths for title, paths in file_groups.items() if paths["metadata"] and paths["content"]}



spark = SparkSession.builder.appName("RevisionProcessing").getOrCreate()

for title, paths in file_groups.items():
    # Load metadata and content
    metadata_df = spark.read.json(paths["metadata"])
    content_df = spark.read.json(paths["content"])

    # Join on `to_id`
    joined_df = content_df.join(metadata_df, content_df["to_id"] == metadata_df["id"], "inner")

    # Group text by `to_id` and aggregate text
    grouped_texts = joined_df.groupBy("to_id").agg(
        collect_list("text").alias("texts")
    )

    # Combine all text into a single document per revision
    grouped_texts = grouped_texts.withColumn("full_text", concat_ws(" ", col("texts")))

    # Save intermediate results
    grouped_texts.write.json(f"/path/to/output/{title}_grouped.json", mode="overwrite")


from pyspark.sql.functions import udf, explode
from pyspark.sql.types import DoubleType, ArrayType, StringType

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

# Save results
scored_df.select("to_id", "uncommon_index").write.csv("/user/s2539829/SHARED_MBD/rev_data/output", header=True, mode="overwrite")
