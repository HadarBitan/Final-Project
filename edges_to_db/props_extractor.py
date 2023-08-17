from configparser import ConfigParser
import os

config = ConfigParser()

# parse existing file
kafka_brokers = "localhost:9092"
# cassandra_client = config.get(section, "cassandra_client")
consumer_group = "consumer_group"  # config.get(section, "consumer_group")
