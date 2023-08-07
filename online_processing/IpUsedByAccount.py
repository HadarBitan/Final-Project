"""
here we do a seperation and sending kafka: account -> ip
all data from kafka would look like this: [number of transfer, account number, ip src, ip dst, amount of transfer, email, props]
number of transfer is a unique key and props would look like this: {"os":"window", "time":324234, "browser": "chrome", "region" : "Canada"}
"""
import kafka_consprod_spark_stream as kaf

# Create a JSON column containing "account" and "email" fields only
json_df = kaf.number_of_transfer.selectExpr("account", "src")

# Select the JSON message to write to Kafka
message_df = json_df.selectExpr("CAST(to_json(struct(*)) AS STRING) as json_message")

# Write the message to Kafka using Spark Structured Streaming
query = message_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kaf.kafka_bootstrap_servers) \
    .option("topic", kaf.kafka_produce_topic) \
    .start()
