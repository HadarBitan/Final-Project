"""
here we do a seperation and sending kafka: account -> email -> region
all data from kafka would look like this: [number of transfer, account number, ip src, ip dst, amount of transfer, email, props]
number of transfer is a unique key and props would look like this: {"os":"window", "time":324234, "browser": "chrome", "region" : "Canada"}
"""
import json

from features_to_db import kafka_consumer, kafka_producer

for message in kafka_consumer.kafka_consumer:
    # Extract the necessary data from Kafka message
    json_m = json.loads(message.value.decode('utf-8')) # maybe u have to change it

    transfer_num = json_m["number of transfer"]
    account_num = json_m["account"]
    src_vertex = json_m["src"]
    dst_vertex = json_m["dst"]
    transfer_value = json_m["value"]
    email = json_m["email"]
    edge_props = json_m["props"]


    # Produce a single message
    message = 'account -> email -> region:' + account_num + ", " + email + ", " + edge_props["region"]
    kafka_producer.producer.produce(kafka_producer.topic, value=message.encode('utf-8'))

    # Wait for the message to be delivered to Kafka
    kafka_producer.producer.flush()
