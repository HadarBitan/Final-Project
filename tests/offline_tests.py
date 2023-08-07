import json
import unittest
from kafka import KafkaProducer, KafkaAdminClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col
from offline_processing import EmailAccount


def _create_kafka_producer(topic):
    # Create a Kafka producer for the specified topic
    kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                   value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    return kafka_producer


def _delete_kafka_topic(topic):
    # Delete the specified Kafka topic
    admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')
    admin_client.delete_topics([topic], timeout_ms=10000, operation_timeout=10000)
    admin_client.close()


class TestEmailAccount(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("TestEmailAccount") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session after all tests are done
        cls.spark.stop()

    def setUp(self):
        # Define the schema for the email_info table
        email_info_schema = StructType([
            StructField("email", StringType(), nullable=False),
            StructField("creation_time", TimestampType(), nullable=False),
            StructField("last_activity_time", TimestampType(), nullable=False)
        ])

        # Create a DataFrame for the email_info table
        email_info_data = [
            ("user1@example.com", "2023-06-01 10:00:00", "2023-06-01 12:00:00"),
            ("user2@example.com", "2023-06-02 11:00:00", "2023-06-02 14:00:00"),
            ("user3@example.com", "2023-06-03 09:00:00", "2023-06-03 16:00:00")
        ]
        self.email_info_df = self.spark.createDataFrame(email_info_data, schema=email_info_schema)

        # Create a Kafka topic for testing
        self.kafka_topic = "test_topic"

        # Write test data to Kafka topic
        test_data = [
            {"email": "user1@example.com", "account_id": 101, "event_time": "2023-06-01 11:30:00"},
            {"email": "user2@example.com", "account_id": 102, "event_time": "2023-06-02 12:30:00"},
            {"email": "user3@example.com", "account_id": 103, "event_time": "2023-06-03 10:30:00"}
        ]
        kafka_producer = _create_kafka_producer(self.kafka_topic)
        for data in test_data:
            kafka_producer.send(self.kafka_topic, value=data)
        kafka_producer.close()

    def tearDown(self):
        # Delete the test topic from Kafka
        _delete_kafka_topic(self.kafka_topic)

    def test_email_account_enrichment(self):
        # Create the EmailAccount instance
        email_account_enricher = EmailAccount()

        # Process events from the test Kafka topic
        email_account_enricher.process_events(self.kafka_topic)

        # Read the enriched data from the Parquet file
        enriched_data = self.spark.read.format("json").load("/tmp/emailAccountEnrichment")

        # Check the enriched data
        expected_data = [
            {"email": "user1@example.com", "account_id": 101, "event_time": "2023-06-01 11:30:00",
             "email_creation_time": "2023-06-01 10:00:00", "email_last_activity_time": "2023-06-01 12:00:00"},
            {"email": "user2@example.com", "account_id": 102, "event_time": "2023-06-02 12:30:00",
             "email_creation_time": "2023-06-02 11:00:00", "email_last_activity_time": "2023-06-02 14:00:00"},
            {"email": "user3@example.com", "account_id": 103, "event_time": "2023-06-03 10:30:00",
             "email_creation_time": "2023-06-03 09:00:00", "email_last_activity_time": "2023-06-03 16:00:00"}
        ]
        self.assertListEqual(enriched_data.collect(), expected_data)


if __name__ == "__main__":
    unittest.main()
