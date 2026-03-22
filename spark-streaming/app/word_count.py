from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

spark = SparkSession.builder \
    .appName("WordCount") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 1. 소켓에서 데이터 읽기
lines = spark.readStream \
    .format("socket") \
    .option("host", "host.docker.internal") \
    .option("port", 9999) \
    .load()

# 2. 단어 단위로 쪼개기
words = lines.select(
    explode(split(col("value"), " ")).alias("word")
)

# [수정 포인트] "spark" 단어만 필터링합니다.
spark_only = words.filter(col("word") == "spark")

# [수정 포인트] words 대신 spark_only를 사용하여 카운트합니다.
word_count = spark_only.groupBy("word").count().orderBy("count", ascending=False)

# 3. 콘솔에 출력
query = word_count.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()