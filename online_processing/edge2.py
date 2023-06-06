"""
here we do a seperation and sending kafka: account -> ip
all data from kafka would look like this: [number of transfer, account number, ip src, ip dst, amount of transfer, email, props]
number of transfer is a unique key and props would look like this: {"os":"window", "time":324234, "browser": "chrome", "region" : "Canada"}
"""
from features_to_db import kafka_consumer, kafka_producer

for message in kafka_consumer.kafka_consumer:
    # Extract the necessary data from Kafka message
    json = message.value.decode('utf-8').split(',')  # maybe u have to change it

    transfer_num = json["number of transfer"]
    account_num = json["account"]
    src_vertex = json["src"]
    dst_vertex = json["dst"]
    transfer_value = json["value"]
    email = json["email"]
    edge_props = json["props"]

    # Produce a single message
    message = 'account -> ip:' + account_num + ", " + src_vertex
    kafka_producer.producer.produce(kafka_producer.topic, value=message.encode('utf-8'))

    # Wait for the message to be delivered to Kafka
    kafka_producer.producer.flush()
