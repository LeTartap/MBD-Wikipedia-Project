from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, to_date, sum as spark_sum

spark = SparkSession.builder.appName("AggregateUncommonWords").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

hdfs_output_dir = "/user/s2539829/SHARED_MBD/rev_data/outputUncommon"

# Load all JSON files into a single DataFrame
df = spark.read.json(f"{hdfs_output_dir}/*.json")

# ensure the schema is correct
df.printSchema()

# extract year and month from the timestamp
df = df.withColumn("year", year(to_date(col("timestamp"))))
df = df.withColumn("month", month(to_date(col("timestamp"))))

# group by year and month, and sum the uncommon word counts
aggregated_df = df.groupBy("year", "month").agg(
    spark_sum("uncommon_word_count").alias("total_uncommon_words")
).orderBy("year", "month")

aggregated_df.show()

aggregated_output_path = "/user/s2539829/SHARED_MBD/rev_data/outputUncommon/aggregated_growth.json"
aggregated_df.write.json(aggregated_output_path, mode="overwrite")
print(f"Aggregated results saved to {aggregated_output_path}")

spark.stop()
