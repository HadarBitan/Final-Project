from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

# Configure Kafka consumer
consumer = KafkaConsumer('events', bootstrap_servers='localhost:9092')

# Configure Spark session
spark = SparkSession.builder.appName('EventProcessing').getOrCreate()

# Configure Cassandra cluster
cluster = Cluster(['localhost'])
session = cluster.connect()

# Create Cassandra keyspace and table
session.execute("CREATE KEYSPACE IF NOT EXISTS event_keyspace WITH REPLICATION = "
                "{'class': 'SimpleStrategy', 'replication_factor': 1}")
session.execute("USE event_keyspace")
session.execute("CREATE TABLE IF NOT EXISTS OfflineEvents (id UUID PRIMARY KEY, value TEXT)")

# Process events from Kafka
for message in consumer:
    event = message.value.decode('utf-8')
    print("Received event:", event)

    # Store event in Cassandra
    session.execute("INSERT INTO OfflineEvents (id, value) VALUES (uuid(), %s)", (event,))

    # Process event using Spark
    df = spark.createDataFrame([(event,)], ['value'])
    df.show()

# Close connections
consumer.close()
spark.stop()
cluster.shutdown()