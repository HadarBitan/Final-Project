from confluent_kafka import Producer

# Kafka broker details
bootstrap_servers = 'localhost:9092'
topic = 'online_edges'

# Create Kafka producer
producer = Producer({'bootstrap.servers': bootstrap_servers})
