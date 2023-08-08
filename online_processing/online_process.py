import json
from abc import abstractmethod

from pyspark.pandas import spark
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import SparkSession
from features_to_db import props_extractor

class online_procees:

    def __init__(self):
        # Create a Spark session
        self.spark = SparkSession.builder \
            .appName("DataEnricher") \
            .getOrCreate()

    def read_from_kafka(self, data_scheme):
        """
        Read data from the Kafka using spark streaming.
        """

        # Read from Kafka using Spark Structured Streaming, we create here a consumer
        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", props_extractor.kafka_brokers) \
            .option("subscribe", props_extractor.consumer_group) \
            .load()

        # Read data from the Kafka topic and create a DataFrame
        data = [json.loads(event.value.decode('utf-8')) for event in kafka_df]
        return data

    def write_to_kafka(self, producer, output):
        """
        A function that gets a topic to write to the new data, and get the data we want to output to kafka
        """
        # Write the message to Kafka using Spark Structured Streaming
        query = output.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", props_extractor.kafka_brokers) \
            .option("topic", producer) \
            .start()

    def extract_evet_type(self, data_json):
        """
        in this function we want to extract from the json we get the event type so we can process the data
        :param data_json: the json we got drom kafka
        :return: a string of the event type
        """
        return data_json.select("data.event_type")

    @abstractmethod
    def extract_data_from_json(self):
        pass
