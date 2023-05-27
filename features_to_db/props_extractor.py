from configparser import ConfigParser
import os

config = ConfigParser()
section = os.getenv("CONFIG_SECTION") #todo make sure u change it to production/local

# parse existing file
kafka_brokers = config.read(section, "kafka_brokers")
cassandra_client = config.read(section, "cassandra_client")
consumer_group = config.read(section, "consumer_group")
