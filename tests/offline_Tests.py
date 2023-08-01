import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from kafka import KafkaProducer, KafkaConsumer
import json
import offline_processing.EmailAccount


# Test the AccountEmail class
def test_account_email_enrichment():
    # Initialize the AccountEmail class
    account_email_enricher = offline_processing.email_account()

    # Test the enrich_data method
    test_data = {
        "email": "test@example.com",
        "account_id": 12345,
        "event_time": "2023-06-01T12:00:00Z"
    }
    enriched_data = account_email_enricher.enrich_data(test_data)
    assert "email_creation_time" in enriched_data
    assert "email_last_activity_time" in enriched_data

    # Test the process_events method
    test_kafka_topic = "test_topic"
    account_email_enricher.process_events(test_kafka_topic)


if __name__ == "__main__":
    # Run the tests
    test_account_email_enrichment()

