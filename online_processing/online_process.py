from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import SparkSession
from features_to_db import props_extractor
import TransactionEvent
import EmailUpdateEvent
import CreditCardAddedEvent
import IPUpdatedEvent
import PhoneUpdatedEvent


class online_process:

    def __init__(self):
        # Create a Spark session
        self.spark = SparkSession.builder \
            .appName("DataEnricher") \
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

        # Convert the value column (Kafka message) to a string
        kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

        # Parse JSON using Spark's built-in JSON functions
        parsed_stream = kafka_df.select(from_json(col("value"), StringType()).alias("parsed_value"))
        return parsed_stream

    @staticmethod
    def write_to_kafka(producer, output):
        """
        A function that gets a topic to write to the new data, and get the data we want to output to kafka
        """
        # Write the message to Kafka using Spark Structured Streaming
        query = output.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", props_extractor.kafka_brokers) \
            .option("topic", producer) \
            .start()

    def process_event(self, data_json):
        """
        in this function we want to extract from the json we get the event type so we can process the data
        :param data_json: the json we got drom kafka
        :return: a string of the event type
        """
        event_type = data_json.select("data.event_type").collect()[0][0]

        event_processor_classes = {
            "Transaction": TransactionEvent.TransactionEventProcessor,
            "Email Update": EmailUpdateEvent.EmailUpdateEventProcessor,
            "Credit Card Add": CreditCardAddedEvent.CreditCardUpdateEventProcessor,
            "IP address Update": IPUpdatedEvent.IPUpdateEventProcessor,
            "Phone Number Update": PhoneUpdatedEvent.PhoneUpdateEventProcessor
        }

        processor_class = event_processor_classes.get(event_type, None)
        if processor_class:
            processor_instance = processor_class(data_json)
            processor_instance.handle()
        else:
            print("Event type not found:", event_type)
