import json
from abc import abstractmethod

from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import SparkContext

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class DataEnricherBase:
    """
    The base class for data enrichment with Kafka and Spark.

    Attributes:
        spark (SparkSession): The Spark session.
    """

    def __init__(self, spark):
        """
        Initializes the DataEnricherBase by creating a Spark session.
        """
        self.spark = spark





    def read_last_hourly_partition_dataframe(self, date, event_name):
        """
        Read data from the Kafka topic and create a DataFrame.

        Args:
            topic_name (str): The name of the Kafka topic.
            kafka_servers (str): The comma-separated list of Kafka server addresses.

        Returns:
            DataFrame: The DataFrame containing the data from the Kafka topic.
        """
        filter_expression = f"hour = {date.hour} and day = {date.day} and year = {date.year} and month = {date.month}"
        return self.spark.table(f"{event_name}_partitioned").filter(filter_expression)

    def join_kafka_with_table(self, date, event_name):
        """
        Perform a JOIN between data from Kafka and an existing table in Spark.

        Args:
            kafka_topic (str): The name of the Kafka topic containing the data.
            table_name (str): The name of the existing table in Spark to join with.
            join_condition (Column): The join condition.

        """
        df_kafka = self.read_last_hourly_partition_dataframe(date, event_name)

        table_df = self.get_enriched_table()

        select_expression = self.get_final_schema_expression() + self.get_relevant_enriched_colums()

        joined_df = df_kafka.join(table_df, on=self.join_by_expression(df_kafka, table_df), how="inner")

        df_select = joined_df.selectExpr(select_expression)

        df_select.write.mode("overwrite").saveAsTable(f"{event_name}_proccesed")


    def get_final_schema_expression(self):
        """
        Get the final schema expression for the enriched data.

        Returns:
            str: The schema expression as a string.
        """
        return f"{self.get_src_column_name()} as src,{self.get_dst_column_name()} as dst,{self.get_timestamp_column_name()} as timestamp,\
        '{self.get_src_type_column_name()}' as src_type,'{self.get_dst_type_column_name()}' as dst_type, '{self.get_edge_type_name()}' as edge_type".split(",")

    def stop_spark_session(self):
        """
        Stop any existing Spark session.
        """
        if 'spark' in globals():
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

    def get_kafka_broker(self):
        return "localhost:9092"
