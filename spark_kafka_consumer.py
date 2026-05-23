from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .load()
 
# Convert binary to string
result = df.selectExpr(
    "CAST(key AS STRING)",
    "CAST(value AS STRING)",
    "topic",
    "partition",
    "offset",
    "timestamp"
)

# Print stream to console
query = result.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()