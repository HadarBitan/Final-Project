from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
import subprocess

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaStructuredStreaming").getOrCreate()

# Define the Kafka broker address and port
kafka_bootstrap_servers = "localhost:9092"

# Define the Kafka topics for consuming and producing
kafka_consume_topic = "demo_cons"
kafka_produce_topic = "demo_prod"

# Define the schema for the Kafka messages
schema = StructType().add("number_of_transfer", IntegerType()) \
                     .add("account", StringType()) \
                     .add("ip_sender", StringType()) \
                     .add("ip_reciver", StringType()) \
                     .add("amount_of_transfer", IntegerType()) \
                     .add("email", StringType()) \
                     .add("props", StringType())

# Read from Kafka using Spark Structured Streaming
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_consume_topic) \
    .load()

# Parse the value column as JSON
parsed_df = kafka_df.select(from_json(col("value"), schema).alias("data"))

# Extract the fields from the parsed JSON data
number_of_transfer = parsed_df.select("data.number_of_transfer")
account = parsed_df.select("data.account")
src = parsed_df.select("data.src")
dst = parsed_df.select("data.dst")
value = parsed_df.select("data.value")
email = parsed_df.select("data.email")
props = parsed_df.select("data.props").alias("props")

# Execute edge1.py
subprocess.call(['python', 'edge1.py'])
# Execute edge2.py
subprocess.call(['python', 'edge2.py'])
# Execute edge3.py
subprocess.call(['python', 'edge3.py'])
if value > 10000:
    # Execute edge4.py
    subprocess.call(['python', 'edge4.py'])
# Execute edge5.py
subprocess.call(['python', 'edge5.py'])


# Start the streaming query
query = number_of_transfer.writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("number_of_transfer_query") \
    .start()

# Wait for the streaming query to finish
query.awaitTermination()
