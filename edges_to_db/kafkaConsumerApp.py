from kafka import KafkaConsumer
from kafkaConfig import KafkaConfig  # Import the Kafka configuration class


class KafkaConsumerApp:
    def __init__(self):
        """
        Initialize the KafkaConsumerApp.
        """
        self.config = KafkaConfig()  # Create an instance of KafkaConfig to get Kafka settings
        self.consumer = KafkaConsumer(
            self.config.topic,
            bootstrap_servers=self.config.kafka_brokers,
            group_id=self.config.consumer_group
        )  # Create a KafkaConsumer instance

    def consume_messages(self):
        """
        Start consuming messages from the Kafka topic.
        """
        for message in self.consumer:
            self.process_message(message)  # Process each consumed message

    def process_message(self, message):
        """
        Process a single Kafka message.

        :param message: The Kafka message to be processed.
        """
        # Implement your message processing logic here
        # This method will be called for each consumed message
        pass


if __name__ == "__main__":
    app = KafkaConsumerApp()  # Create an instance of KafkaConsumerApp
    app.consume_messages()  # Start consuming messages from the Kafka topic
