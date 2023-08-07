from abc import abstractmethod

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

    def get_final_schema_expression(self):
        return f"${self.get_src_column_name()} as src, ${self.get_dst_column_name()} as dst, ${self.get_timestamp_column_name()} as timestamp, " + \
               f"${self.get_src_type_column_name()} as src_type, ${self.get_dst_type_column_name()} as dst_type, ${self.get_edge_type_name()} as edge_type"

    def stop_spark_session(self):
        # Stop the Spark session
        self.spark.stop()

    """
    Return a list of relevant events for data enrichment.

    Returns:
    list: A list of event names.
    """
    @abstractmethod
    def get_relevant_events_list(self):
        pass

    """
    Define the join condition expression for data enrichment.

    Returns:
        str: The join condition expression.
    """
    @abstractmethod
    def join_by_expression(self):
        pass

    """
    Get the Spark DataFrame of the enriched data table.

    Returns:
        DataFrame: The Spark DataFrame of the enriched data table.
    """
    @abstractmethod
    def get_enriched_table(self):
        pass

    """
    Get a list of relevant columns from the enriched data table.

    Returns:
        list: A list of column names.
    """
    @abstractmethod
    def get_relevant_enriched_columns(self):
        pass

    """
    Get the name of the source column for join.

    Returns:
        str: The name of the source column.
    """
    @abstractmethod
    def get_src_column_name(self):
        pass

    """
    Get the name of the source type column for join.

    Returns:
    str: The name of the source type column.
    """
    @abstractmethod
    def get_src_type_column_name(self):
        pass

    """
    Get the name of the destination column in the enriched data table.

    Returns:
        str: The name of the destination column.
    """
    @abstractmethod
    def get_dst_column_name(self):
        pass

    """
    Get the name of the destination type column in the enriched data table.

    Returns:
        str: The name of the destination type column.
    """
    @abstractmethod
    def get_dst_type_column_name(self):
        pass

    """
        Get the name of the timestamp column in the enriched data table.

        Returns:
            str: The name of the timestamp column.
        """
    @abstractmethod
    def get_timestamp_column_name(self):
        pass

    """
    Get the name of the destination column for join.

    Returns:
        str: The name of the destination column.
    """
    @abstractmethod
    def get_edge_type_name(self):
        pass







