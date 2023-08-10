from confluent_kafka import Producer
from kafkaConfig import KafkaConfig  # Import the Kafka configuration class


def create_kafka_producer(topic):
    """
    Create a Kafka producer for the specified topic.

    :param topic: The topic to produce to.
    :return: A Producer instance.
    """
    bootstrap_servers = 'localhost:9092'
    kafka_config = KafkaConfig(bootstrap_servers, topic, None)  # Pass None as consumer_group since it's not used here

    return Producer({'bootstrap.servers': kafka_config.bootstrap_servers})
