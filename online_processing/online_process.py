from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
from pyspark.sql import SparkSession
from edges_to_db import props_extractor
from TransactionEvent import TransactionEventProcessor
from  EmailUpdateEvent import EmailUpdateEventProcessor
from CreditCardAddedEvent import CreditCardUpdateEventProcessor
from IPUpdatedEvent import IPUpdateEventProcessor
from PhoneUpdatedEvent import PhoneUpdateEventProcessor


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


        # Define the schema for the Kafka DataFrame
        kafka_schema = StructType([
            StructField("event_type", StringType(), nullable=False)
        ])

        # Convert Kafka value (JSON string) to a struct using the known schema
        parsed_stream = kafka_df \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json("value", kafka_schema).alias("data")) \
            .select("data.*")
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
        event_type_value = None
        event_type = data_json.select("event_type").first()
        if event_type:
            event_type_value = event_type[0].lower().replace(" ", "")

        event_processor_classes = {
            "transaction": TransactionEventProcessor,
            "emailupdate": EmailUpdateEventProcessor,
            "creditcardadd": CreditCardUpdateEventProcessor,
            "ipaddressupdate": IPUpdateEventProcessor,
            "phonenumberupdate": PhoneUpdateEventProcessor
        }

        print(event_type_value)
        processor_class = event_processor_classes.get(event_type_value, None)
        if processor_class:
            processor_instance = processor_class(self, data_json)
            processor_instance.handle()
        else:
            print("Event type not found:", event_type)
