from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType


def spark_read_from_kafka():
    #todo read json from kafka
    # Set the schema for parsing the JSON value
    schema = StructType().add("src", StringType()) \
        .add("dst", StringType()) \
        .add("value", StringType()) \
        .add("props", StringType()) \
        .add("src_type", StringType()) \
        .add("dst_type", StringType()) \
        .add("edge_type", StringType()) \
        .add("mode", StringType())

    # Create a SparkSession
    spark = SparkSession.builder.appName("OfflineSystem").getOrCreate()

    # Read data from Kafka topic
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "transactionRequest"
    kafka_starting_offsets = "earliest"

    kafka_df = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("failOnDataLoss", "false") \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", kafka_starting_offsets) \
        .option("spark.streaming.kafka.maxRatePerPartition", "50") \
        .load()

    # Extract the value column as a string
    kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

    # Parse the JSON value using the defined schema
    parsed_df = kafka_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    return parsed_df


# Usage
data = spark_read_from_kafka()
data.show()
