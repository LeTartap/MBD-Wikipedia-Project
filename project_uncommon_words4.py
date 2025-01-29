from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, collect_list, lower, regexp_replace, size, split
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("CountUncommonWords").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

hdfs_directory = "/user/s2539829/SHARED_MBD/rev_data"

common_words_csv = "combined_chatgpt_words.csv"

# load the list of common ChatGPT words using Spark
common_words_df = spark.read.csv(common_words_csv, header=False).toDF("word")
common_words = set(row["word"].lower() for row in common_words_df.collect())

# Broadcast the common words set to all Spark workers
common_words_broadcast = spark.sparkContext.broadcast(common_words)

def count_uncommon_words(text):
    if not text:
        return 0
    words = text.split()
    uncommon_count = sum(1 for word in words if word not in common_words_broadcast.value)
    return uncommon_count

count_uncommon_words_udf = spark.udf.register("count_uncommon_words", count_uncommon_words, IntegerType())

# dynamically list all relevant titles in the directory
all_files = [f.path for f in spark.read.format("binaryFile").load(f"{hdfs_directory}/*_revisions.json").select("path").collect()]

def get_title_from_path(file_path):
    return file_path.split("/")[-1].replace("_revisions.json", "")

titles = [get_title_from_path(file) for file in all_files]

# process each file group
for title in titles:
    metadata_path = f"{hdfs_directory}/{title}_revisions.json"
    content_path = f"{hdfs_directory}/{title}_revision_content.json"

    # load metadata and content
    metadata_df = spark.read.json(metadata_path)
    content_df = spark.read.json(content_path)

    # join on `to_id` and group text by `to_id`
    joined_df = content_df.join(metadata_df, content_df["to_id"] == metadata_df["id"], "inner")
    grouped_texts = joined_df.groupBy("to_id", "timestamp").agg(
        concat_ws(" ", collect_list("text")).alias("full_text")
    )

    # clean and tokenize text, then count uncommon words
    grouped_texts = grouped_texts.withColumn(
        "clean_text",
        regexp_replace(lower(col("full_text")), "[^a-zA-Z\s]", "")
    )
    grouped_texts = grouped_texts.withColumn(
        "uncommon_word_count", count_uncommon_words_udf(col("clean_text"))
    )

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
