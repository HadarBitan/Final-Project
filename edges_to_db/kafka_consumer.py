# Set up Kafka consumer
from kafka import KafkaConsumer

from edges_to_db import props_extractor

kafka_consumer = KafkaConsumer(
    'kafka_to_cassandra',
    bootstrap_servers=props_extractor.kafka_brokers,
    group_id=props_extractor.consumer_group
)
