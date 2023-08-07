from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import SparkSession
import subprocess

# Create a SparkSession
spark = SparkSession.builder\
    .appName("KafkaStructuredStreaming")\
    .config("spark.sql.shuffle.partitions", "1")\
    .config("spark.driver.memory", "1g")\
    .config("spark.executor.memory", "1g")\
    .config("spark.executor.cores", "1")\
    .config("spark.executor.instances", "1")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()
"""
config("spark.sql.shuffle.partitions", "1") - You can set this for better performance in a production setup
config("spark.driver.memory", "1g") -  Adjust driver memory as needed
config("spark.executor.memory", "1g") - Adjust executor memory as needed
config("spark.executor.cores", "1") - Adjust the number of cores per executor as needed
config("spark.executor.instances", "1") - Adjust the number of executor instances as needed
"""

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
#number_of_transfer:265482,account:A5fV2,ip_sender:123.12.0.25,ip_reciver:125.63.5.23,amount_of_transfer:500,email:aaaa@gmail.com,props:{"os":"window", "time":324234, "browser": "chrome", "region" : "Canada"}
# Read from Kafka using Spark Structured Streaming
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_produce_topic) \
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

# Execute EmailUsedByAccount.py
subprocess.call(['python', 'EmailUsedByAccount.py'])
# Execute IpUsedByAccount.py
# subprocess.call(['python', 'IpUsedByAccount.py'])
# # Execute IpSrcUsedByNumberOfTransfer.py
# subprocess.call(['python', 'IpSrcUsedByNumberOfTransfer.py'])
# if value > 10000:
#     # Execute ExceptionUsedByNumberOfTransfer.py
#     subprocess.call(['python', 'ExceptionUsedByNumberOfTransfer.py'])
# # Execute AmountOfTransferUsedByNumberOfTransfer.py
# subprocess.call(['python', 'AmountOfTransferUsedByNumberOfTransfer.py'])


# Start the streaming query
query = number_of_transfer.writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("number_of_transfer_query") \
    .start()

# Wait for the streaming query to finish
query.awaitTermination()
