import threading

from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from cassandra.cluster import Cluster

# Configure Kafka consumer
consumer = KafkaConsumer('events', bootstrap_servers='localhost:9092')

# Configure Spark Streaming context
spark = SparkSession.builder.appName('EventProcessing').getOrCreate()
ssc = StreamingContext(spark.sparkContext, 1)

# Configure Cassandra cluster
cluster = Cluster(['localhost'])
session = cluster.connect()

# Create Cassandra keyspace and table
session.execute("CREATE KEYSPACE IF NOT EXISTS event_keyspace WITH REPLICATION = "
                "{'class': 'SimpleStrategy', 'replication_factor': 1}")
session.execute("USE event_keyspace")
session.execute("CREATE TABLE IF NOT EXISTS OnlineEvents (id UUID PRIMARY KEY, value TEXT)")

# Process events from Kafka
def process_event(event):
    event_value = event.value.decode('utf-8')
    print("Received event:", event_value)

    # Store event in Cassandra
    session.execute("INSERT INTO events (id, value) VALUES (uuid(), %s)", (event_value,))

    # Process event using Spark
    df = spark.createDataFrame([(event_value,)], ['value'])
    df.show()

# Create Kafka consumer thread
def consume_events():
    for event in consumer:
        process_event(event)

# Start Kafka consumer thread
consumer_thread = threading.Thread(target=consume_events)
consumer_thread.start()

# Start the streaming context
ssc.start()
ssc.awaitTermination()

# Close connections
consumer_thread.join()
consumer.close()
spark.stop()
cluster.shutdown()
