from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaSparkTest") \
    .getOrCreate()

print("Spark is working!")

spark.stop()
