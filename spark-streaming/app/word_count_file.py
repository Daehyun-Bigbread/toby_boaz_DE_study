from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

spark = SparkSession.builder \
    .appName("WordCountFile") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = "value STRING"

lines = spark.readStream \
    .format("text") \
    .schema(schema) \
    .option("path", "/app/data") \
    .load()

words = lines.select(
    explode(split(col("value"), " ")).alias("word")
)

word_count = words.groupBy("word").count()

query = word_count.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()