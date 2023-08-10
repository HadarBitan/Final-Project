import json
from abc import abstractmethod

from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


class DataEnricherBase:
    """
    The base class for data enrichment with Kafka and Spark.

    Attributes:
        spark (SparkSession): The Spark session.
    """

    def __init__(self):
        """
        Initializes the DataEnricherBase by creating a Spark session.
        """
        self.spark = SparkSession.builder \
            .appName("DataEnricher") \
            .getOrCreate()

    def read_kafka_topic_as_dataframe(self, topic_name, kafka_servers):
        """
        Read data from the Kafka topic and create a DataFrame.

        Args:
            topic_name (str): The name of the Kafka topic.
            kafka_servers (str): The comma-separated list of Kafka server addresses.

        Returns:
            DataFrame: The DataFrame containing the data from the Kafka topic.
        """
        kafka_consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_servers)

        data = [json.loads(event.value.decode('utf-8')) for event in kafka_consumer]
        return self.spark.createDataFrame(data)

    def join_kafka_with_table(self, kafka_topic, table_name, join_condition):
        """
        Perform a JOIN between data from Kafka and an existing table in Spark.

        Args:
            kafka_topic (str): The name of the Kafka topic containing the data.
            table_name (str): The name of the existing table in Spark to join with.
            join_condition (Column): The join condition.

        """
        df_kafka = self.read_kafka_topic_as_dataframe(kafka_topic).filter(col("event_type").isin(self.get_relevant_events_list()))

        table_df = self.get_enriched_table()

        joined_df = df_kafka.join(table_df, on=self.join_by_expression(), how="inner").select(self.get_final_schema_expression() + "," + self.get_relevant_events_list())

        joined_df.write.format("json").mode("append").save("/tmp/enriched_data")

    def get_final_schema_expression(self):
        """
        Get the final schema expression for the enriched data.

        Returns:
            str: The schema expression as a string.
        """
        return f"${self.get_src_column_name()} as src, ${self.get_dst_column_name()} as dst, ${self.get_timestamp_column_name()} as timestamp, +\
        ${self.get_src_type_column_name()} as src_type, ${self.get_dst_type_column_name()} as dst_type, ${self.get_edge_type_name()} as edge_type"

    def stop_spark_session(self):
        """
        Stop the Spark session.
        """
        self.spark.stop()

    @abstractmethod
    def get_relevant_events_list(self):
        """
        Get the list of relevant events for the data enrichment.

        Returns:
            list: A list of relevant event names.
        """
        pass

    @abstractmethod
    def join_by_expression(self):
        """
        Get the expression for the JOIN between Kafka data and existing Spark table.

        Returns:
            str: The expression for the JOIN condition.
        """
        pass

    @abstractmethod
    def get_enriched_table(self):
        """
        Get the Spark DataFrame of the enriched data.

        Returns:
            DataFrame: The DataFrame of the enriched data.
        """
        pass

    @abstractmethod
    def get_relevant_enriched_columns(self):
        """
        Get the list of relevant columns for the enriched data.

        Returns:
            list: A list of relevant column names.
        """
        pass

    @abstractmethod
    def get_src_column_name(self):
        """
        Get the source column name for the enrichment.

        Returns:
            str: The source column name.
        """
        pass

    @abstractmethod
    def get_src_type_column_name(self):
        """
        Get the source type column name for the enrichment.

        Returns:
            str: The source type column name.
        """
        pass

    @abstractmethod
    def get_dst_column_name(self):
        """
        Get the destination column name for the enrichment.

        Returns:
            str: The destination column name.
        """
        pass

    @abstractmethod
    def get_dst_type_column_name(self):
        """
        Get the destination type column name for the enrichment.

        Returns:
            str: The destination type column name.
        """
        pass

    @abstractmethod
    def get_timestamp_column_name(self):
        """
        Get the timestamp column name for the enrichment.

        Returns:
            str: The timestamp column name.
        """
        pass

    @abstractmethod
    def get_edge_type_name(self):
        """
        Get the edge type name for the enrichment.

        Returns:
            str: The edge type name.
        """
        pass
