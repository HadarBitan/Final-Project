from confluent_kafka import Producer
from kafkaConfig import KafkaConfig  # Import the Kafka configuration class


class KafkaProducerApp:
    def __init__(self):
        """
        Initialize the KafkaProducerApp.
        """
        self.config = KafkaConfig()  # Create an instance of KafkaConfig to get Kafka settings
        self.producer = Producer({'bootstrap.servers': self.config.bootstrap_servers})
        # Create a Kafka Producer instance with the specified bootstrap servers

    def produce_messages(self):
        """
        Produce a series of messages to the Kafka topic.
        """
        for _ in range(10):
            self.producer.produce(self.config.topic, key='key', value='message')
            # Produce a message with a constant key and value
            self.producer.flush()  # Flush the producer buffer to ensure message delivery


if __name__ == "__main__":
    app = KafkaProducerApp()  # Create an instance of KafkaProducerApp
    app.produce_messages()  # Produce a series of messages to the Kafka topic
