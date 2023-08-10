from kafka import KafkaConsumer
from kafkaConfig import KafkaConfig  # Import the Kafka configuration class


def create_kafka_consumer(topic):
    """
    Create a Kafka consumer for the specified topic.

    :param topic: The topic to consume from.
    :return: A KafkaConsumer instance.
    """
    bootstrap_servers = 'localhost:9092'
    kafka_config = KafkaConfig(bootstrap_servers, topic, None)  # Pass None as consumer_group since it's not used here

    return KafkaConsumer(
        kafka_config.topic,
        bootstrap_servers=kafka_config.bootstrap_servers,
        group_id=kafka_config.consumer_group
    )
