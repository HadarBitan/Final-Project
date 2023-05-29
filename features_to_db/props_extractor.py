from configparser import ConfigParser
import os

config = ConfigParser()
section = os.getenv("local_mode") 

# parse existing file
kafka_brokers = config.get(section, "kafka_brokers")
cassandra_client = config.get(section, "cassandra_client")
consumer_group = config.get(section, "consumer_group")

