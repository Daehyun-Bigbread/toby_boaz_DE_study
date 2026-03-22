from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# 1. SparkSession 생성
spark = SparkSession.builder \
    .appName("HDFS_WordCount") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# 2. HDFS에서 데이터 읽기
text_file = spark.read.text("hdfs://namenode:9000/input/sample.txt")

# 3. WordCount 계산 (DataFrame API)
words = text_file.select(explode(split(text_file.value, " ")).alias("word"))
wordCounts = words.groupBy("word").count().orderBy("count", ascending=False).limit(3)

# 4. 결과 출력
print("========== Result ==========")
wordCounts.show()
print("============================")

spark.stop()