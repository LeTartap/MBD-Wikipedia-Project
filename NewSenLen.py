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
df_variance = df_exploded.groupBy("date_to_id").agg(variance(col("word_count")).alias("variance"))

df_variance.show(5)
