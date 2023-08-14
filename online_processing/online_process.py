from kafka import KafkaProducer
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
from pyspark.sql import SparkSession
from edges_to_db import props_extractor
# from TransactionEvent import TransactionEventProcessor
# from  EmailUpdateEvent import EmailUpdateEventProcessor
# from CreditCardAddedEvent import CreditCardUpdateEventProcessor
# from IPUpdatedEvent import IPUpdateEventProcessor
# from PhoneUpdatedEvent import PhoneUpdateEventProcessor


class OnlineProcess:

    def __init__(self):
        # Create a Spark session
        self.spark = SparkSession.builder \
            .appName("KafkaSparkIntegration") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
            .getOrCreate()

    def read_from_kafka(self):
        """
        Read data from the Kafka using spark streaming.
        """

        # Read from Kafka using Spark Structured Streaming, we create here a consumer
        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", props_extractor.kafka_brokers) \
            .option("subscribe", props_extractor.consumer_group) \
            .load()

        parsed_stream = kafka_df.selectExpr("CAST(value AS STRING)")
        return parsed_stream

    def write_to_kafka(self, producer_topic, output):
        """
        A function that gets a topic to write to the new data, and get the data we want to output to kafka
        """
        # Create a Kafka producer instance
        producer = KafkaProducer(bootstrap_servers=props_extractor.kafka_brokers)
        # Send the JSON output as a Kafka message
        producer.send(producer_topic, output.encode('utf-8'))
        # Close the Kafka producer to ensure proper resource management and prevent resource leaks
        producer.close()
