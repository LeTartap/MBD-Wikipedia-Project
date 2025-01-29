from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, collect_list, lower, regexp_replace, size, split, year, month, to_date, sum as spark_sum
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("ProcessAndAggregateUncommonWords").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Path to the HDFS  containing input JSON files
hdfs_directory = "/user/s2539829/SHARED_MBD/rev_data"

common_words_csv = "combined_chatgpt_words.csv"

common_words_df = spark.read.csv(common_words_csv, header=False).toDF("word")
common_words = set(row["word"].lower() for row in common_words_df.collect())

common_words_broadcast = spark.sparkContext.broadcast(common_words)

def count_uncommon_words(text):
    if not text:
        return 0
    words = text.split()
    uncommon_count = sum(1 for word in words if word not in common_words_broadcast.value)
    return uncommon_count

count_uncommon_words_udf = spark.udf.register("count_uncommon_words", count_uncommon_words, IntegerType())

all_files = [f.path for f in spark.read.format("binaryFile").load(f"{hdfs_directory}/*_revisions.json").select("path").collect()]

def get_title_from_path(file_path):
    return file_path.split("/")[-1].replace("_revisions.json", "")

titles = [get_title_from_path(file) for file in all_files]

aggregated_df = None

#  aggregate all file groups
for title in titles:
    metadata_path = f"{hdfs_directory}/{title}_revisions.json"
    content_path = f"{hdfs_directory}/{title}_revision_content.json"

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

    # extract year and month 
    grouped_texts = grouped_texts.withColumn("year", year(to_date(col("timestamp"))))
    grouped_texts = grouped_texts.withColumn("month", month(to_date(col("timestamp"))))

    # aggregate uncommon word counts by year and month
    aggregated_partial_df = grouped_texts.groupBy("year", "month").agg(
        spark_sum("uncommon_word_count").alias("total_uncommon_words")
    )

    # combine 
    if aggregated_df is None:
        aggregated_df = aggregated_partial_df
    else:
        aggregated_df = aggregated_df.union(aggregated_partial_df)

# aggregation to consolidate results across all titles
final_aggregated_df = aggregated_df.groupBy("year", "month").agg(
    spark_sum("total_uncommon_words").alias("total_uncommon_words")
).orderBy("year", "month")

# show the aggregated result
final_aggregated_df.show()

# save  to HDFS
final_output_path = "/user/s2539829/SHARED_MBD/rev_data/outputUncommon/aggregated_growth.json"
final_aggregated_df.write.json(final_output_path, mode="overwrite")
print(f"Final aggregated results saved to {final_output_path}")

spark.stop()
