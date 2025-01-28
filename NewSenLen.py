from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import col, udf, when, length, size, regexp_replace,split,concat_ws,flatten,collect_list, variance,explode
from pyspark.sql.types import BooleanType, ArrayType, StringType, IntegerType
from pyspark.sql.functions import flatten

spark = SparkSession.builder.appName("Wikipedia Sentence Analysis").getOrCreate()

# Load only _revision_content.json files
data_path = "/user/s2539829/SHARED_MBD/rev_data/*_revision_content.json"
df = spark.read.json(data_path)

df = df.select("date", "to_id", "text")

df = df.withColumn("text", when(col("text").isNotNull(), regexp_replace(col("text"), r'[^\w\s.,!?]', ' ')))
df = df.filter(col("text") != "")
# split text into senteces the divider being . ! or ?
df = df.withColumn("sentences", split(col("text"), r'(?<=[.!?]) +'))
# get word count for each sentence
word_count_udf = udf(lambda sentences: [len(sentence.split()) for sentence in sentences], ArrayType(IntegerType()))
df = df.withColumn("sentence_word_counts", word_count_udf(col("sentences")))

df = df.withColumn("date_to_id", concat_ws("_", col("to_id"), col("date")))
df = df.drop("date", "to_id","text","sentences")

df_exploded = df.withColumn("word_count", explode(col("sentence_word_counts")))
df_mean = df_exploded.groupBy("date_to_id").agg({"word_count": "mean"})
df_mean.show(5)
df_variance = df_exploded.groupBy("date_to_id").agg(variance(col("word_count")).alias("variance"))
#merge the two dataframes
df_stats = df_mean.join(df_variance, "date_to_id")
#calculate the variance to mean ratio
df_stats = df_stats.withColumn("relative_variance", col("variance") / col("avg(word_count)"))

df_stats = df_stats.withColumn("to_id", split(col("date_to_id"), "_")[0])
df_stats = df_stats.withColumn("date", split(col("date_to_id"), "_")[1])
df_stats = df_stats.drop("date_to_id")
df_stats.show(5)
#add header to the csv
header = ["to_id", "date", "avg(word_count)", "variance", "relative_variance"]
#save the dataframe to a csv file
df_stats.write.mode("overwrite").csv("/user/s2539829/SHARED_MBD/rev_data_SenLen", header=True)
