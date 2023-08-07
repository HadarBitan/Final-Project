from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from kafka import KafkaConsumer
import json
from abc import abstractmethod



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

    def read_kafka_topic_as_dataframe(self, topic_name, kafka_servers):
        """
        Read data from the Kafka topic and create a DataFrame.

        Args:
            topic_name (str): The name of the Kafka topic.

        Returns:
            DataFrame: The DataFrame containing the data from the Kafka topic.
        """
        # Create a Kafka consumer for the specified topic
        kafka_consumer = KafkaConsumer(topic_name, bootstrap_servers = kafka_servers)

        # Read data from the Kafka topic and create a DataFrame
        data = [json.loads(event.value.decode('utf-8')) for event in kafka_consumer]
        return self.spark.createDataFrame(data).filter(s"


    def join_kafka_with_table(self, kafka_topic, table_name, join_condition):
        """
        Perform a JOIN between data from Kafka and an existing table in Spark.

        Args:
            kafka_topic (str): The name of the Kafka topic containing the data.
            table_name (str): The name of the existing table in Spark to join with.
            join_condition (Column): The join condition.

        """
        # Read data from Kafka topic and create DataFrame
        df = self.read_kafka_topic_as_dataframe(kafka_topic).filter(col("event_type).in(self.get_relevant_events_list()))

        # Read data from the existing DataFrame (specified table)
        table_df = get_enriched_table()

        # Perform the JOIN on the specified condition
        joined_df = df_kafka.join(table_df, on=self.join_by_expression(), how="inner").select(self.get_final_schema_expression() + "," + self.get_relevant_events_list())

  

        # Write the enriched data to Delta Lake, perform additional transformations, etc.
        enriched_data.write.format("json").mode("append").save("/tmp/enriched_data")

    def get_final_schema_expression(self):
        "${self.get_src_column_name()} as src, ${self.get_dst_column_name()} as dst, ${self.get_timestamp_column_name()} as timestamp, +\
        ${self.get_src_type_column_name()} as src_type, ${self.get_dst_type_column_name()} as dst_type, ${self.get_edge_type_name()} as edge_type"

    def stop_spark_session(self):
        # Stop the Spark session
        self.spark.stop()
        
    @abstractmethod
    def get_relevant_events_list(self):
        pass
    @abstractmethod
    def join_by_expression(self):
        pass
    @abstractmethod
    def get_enriched_table(self):
        pass
    @abstractmethod
    def get_relevant_enriched_colums(self):
        pass
        
    @abstractmethod
    def get_src_column_name(self):
        pass
        
    @abstractmethod
    def get_src_type_column_name(self):
        pass
        
    @abstractmethod
    def get_dst_column_name(self):
        pass    

    @abstractmethod
    def get_dst_type_column_name(self):
        pass  
        
    @abstractmethod
    def get_timestamp_column_name(self):
        pass    

    @abstractmethod
    def get_edge_type_name(self):
        pass            
