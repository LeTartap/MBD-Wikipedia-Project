from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, split
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import concat_ws

spark = SparkSession.builder.appName("example").getOrCreate()
singleRevision = spark.read.json("/user/s2539829/SHARED_MBD/rev_data")
#get length of revisions
singleRevision.count()
#return a table that includes the column "date", "text", "title", "to_id"
newTable = singleRevision.select("date", "text", "to_id")
#count the number of characters in the text column
newTable = newTable.withColumn("text_length", length(col("text")))
newTable = newTable.withColumn("date_to_id", concat_ws("_", col("to_id"), col("date")))
newTable = newTable.groupBy("date_to_id").sum("text_length")
#split the column "date_to_id" into two columns "to_id" and "date"
newTable = newTable.withColumn("to_id", split(col("date_to_id"), "_")[0])
newTable = newTable.withColumn("date", split(col("date_to_id"), "_")[1])
newTable = newTable.drop("date_to_id")
#save to csv 
newTable.write.mode("overwrite").csv("/user/s2539829/SHARED_MBD/rev_data_csv")





