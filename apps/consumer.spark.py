import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# --- CONFIGURATION ---
# CRITICAL FIX: Use internal Kafka port 29092 for inter-container communication
KAFKA_BOOTSTRAP_SERVER = "kafka:29092"
# Topic name must match the one created by kafka-topic-creator ("stock_data")
KAFKA_TOPIC = "stock_data" 
WAIT_SECONDS = 90 # Increased wait time for better cluster stability

# ADJUSTMENT: Significantly increase the timeout for Kafka broker responses.
KAFKA_REQUEST_TIMEOUT_MS = "120000"
KAFKA_SESSION_TIMEOUT_MS = "30000" # Also necessary for consumer group stability

# 1. Define the topic data schema (must match producer output)
STOCK_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("volume", DoubleType(), True)
])

def wait_for_topic_creator(wait_time):
    """
    A grace period wait for Kafka to fully stabilize and register metadata.
    This helps mitigate race conditions on startup.
    """
    print(f"--- WAITING {wait_time} SECONDS for final topic readiness defense ---")
    time.sleep(wait_time)
    print("--- STARTING SPARK STREAM ---")

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("StockMarketConsumer") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()
    
    # Set Spark log level to ERROR to reduce console clutter
    spark.sparkContext.setLogLevel("ERROR")

    # --- FINAL SAFETY WAIT ---
    wait_for_topic_creator(WAIT_SECONDS)

    # 2. Read from Kafka using Structured Streaming
    kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("kafka.request.timeout.ms", KAFKA_REQUEST_TIMEOUT_MS) \
        .option("kafka.session.timeout.ms", KAFKA_SESSION_TIMEOUT_MS) \
        .load()

    # 3. Deserialize the Kafka value (which is binary) and apply the schema
    parsed_stream = kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), STOCK_SCHEMA).alias("data")) \
        .select("data.*")

    # 4. Process the data (e.g., print to console)
    query = parsed_stream \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    print("Spark Structured Streaming is running. Press Ctrl+C to stop...")
    query.awaitTermination()
