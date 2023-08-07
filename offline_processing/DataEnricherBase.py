from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from kafka import KafkaConsumer
import json


class DataEnricherBase:
    """
    The base class for data enrichment with Kafka and Spark.

    Attributes:
        spark (SparkSession): The Spark session.
    """

    def __init__(self):
        # Create a Spark session
        self.spark = SparkSession.builder \
            .appName("DataEnricher") \
            .getOrCreate()

    def read_kafka_topic_as_dataframe(self, topic_name):
        """
        Read data from the Kafka topic and create a DataFrame.

        Args:
            topic_name (str): The name of the Kafka topic.

        Returns:
            DataFrame: The DataFrame containing the data from the Kafka topic.
        """
        # Create a Kafka consumer for the specified topic
        kafka_consumer = KafkaConsumer(topic_name, bootstrap_servers='localhost:9092')

        # Read data from the Kafka topic and create a DataFrame
        data = [json.loads(event.value.decode('utf-8')) for event in kafka_consumer]
        return self.spark.createDataFrame(data)

    def enrich_data(self, data):
        """
        Enrich the data as needed.
        This method should be implemented in the derived classes.

        Args:
            data (dict): The data to be enriched.

        Returns:
            dict: The enriched data.
        """
        raise NotImplementedError("The enrich_data method must be implemented in the derived classes.")

    def join_kafka_with_table(self, kafka_topic, table_name, join_condition):
        """
        Perform a JOIN between data from Kafka and an existing table in Spark.

        Args:
            kafka_topic (str): The name of the Kafka topic containing the data.
            table_name (str): The name of the existing table in Spark to join with.
            join_condition (Column): The join condition.

        """
        # Read data from Kafka topic and create DataFrame
        df_kafka = self.read_kafka_topic_as_dataframe(kafka_topic)

        # Read data from the existing DataFrame (specified table)
        table_df = self.spark.table(table_name)

        # Perform the JOIN on the specified condition
        joined_df = df_kafka.join(table_df, on=join_condition, how="inner")

        # Enrich the data with the specified DataFrame
        enriched_data = joined_df.rdd.map(self.enrich_data).toDF()

        # Write the enriched data to Delta Lake, perform additional transformations, etc.
        enriched_data.write.format("json").mode("append").save("/tmp/enriched_data")

    def stop_spark_session(self):
        # Stop the Spark session
        self.spark.stop()

