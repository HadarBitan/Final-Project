"""
here we do a seperation and sending kafka: account -> number of transfer that is over 10,000
all data from kafka would look like this: [number of transfer, account number, ip src, ip dst, amount of transfer, email, props]
number of transfer is a unique key and props would look like this: {"os":"window", "time":324234, "browser": "chrome", "region" : "Canada"}
"""
from pyspark.sql.functions import from_json, col, concat_ws
import kafka_consprod_spark_stream as kaf

# Create the message to write to Kafka
message = concat_ws(", ", "Transfer Exception:", kaf.account, kaf.number_of_transfer).alias("message")

# Select the message to write to Kafka
message_df = message.selectExpr("CAST(message AS STRING)")

# Write the message to Kafka using Spark Structured Streaming
query = message_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kaf.kafka_bootstrap_servers) \
    .option("topic", kaf.kafka_produce_topic) \
    .start()
