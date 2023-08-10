from kafka import KafkaConsumer
from confluent_kafka import Producer


class KafkaConfig:
    def __init__(self, bootstrap_servers, topic, consumer_group):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer_group = consumer_group


def create_kafka_consumer(topic):
    """
    Create a Kafka consumer for the specified topic.

    :param topic: The topic to consume from.
    :return: A KafkaConsumer instance.
    """
    bootstrap_servers = 'localhost:9092'
    consumer_group = 'my_consumer_group'
    kafka_config = KafkaConfig(bootstrap_servers, topic, consumer_group)

    return KafkaConsumer(
        kafka_config.topic,
        bootstrap_servers=kafka_config.bootstrap_servers,
        group_id=kafka_config.consumer_group
    )


def create_kafka_producer(topic):
    """
    Create a Kafka producer for the specified topic.

    :param topic: The topic to produce to.
    :return: A Producer instance.
    """
    bootstrap_servers = 'localhost:9092'

    return Producer({'bootstrap.servers': bootstrap_servers})



