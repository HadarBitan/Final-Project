from edges_to_db import metric_container

from cassandra.cluster import Cluster

from edges_to_db.kafka_consumer import create_kafka_consumer

cluster = Cluster(['127.0.0.1'], port=9042)
cassandra_session = cluster.connect()

def create_keyspace(cassandra_session,key_space_name):
    create_keyspace_query =f"CREATE KEYSPACE {key_space_name} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }"

    cassandra_session.execute(create_keyspace_query)

def create_table(cassandra_session,key_space, table_name):
    create_table = f"CREATE TABLE IF NOT EXISTS {key_space}.{table_name}  (src text,dst text, timestamp text, PRIMARY KEY (src))"

    cassandra_session.execute(create_table)


# Define the keyspace name and replication options
keyspace_name = "my_keyspace"

prepare_statement_container = {}
for name, table in metric_container.features_to_tables.items():
    create_table(cassandra_session, keyspace_name, table)
    # prepare_statement_container[name] = cassandra_session.prepare(query.format(table))

# Read messages from Kafka and write to Cassandra
kafka_consumer = create_kafka_consumer("omer")



from confluent_kafka import Consumer, KafkaError

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
    'group.id': 'my-consumer-group',  # Replace with a unique consumer group id
    'auto.offset.reset': 'earliest'  # Start consuming from the earliest available offset
}

# Create a Kafka consumer instance
consumer = Consumer(kafka_config)

# Subscribe to the topic(s) you want to consume from
topic = 'omer'  # Replace with the topic you want to consume from
consumer.subscribe([topic])

try:
    while True:
        # Poll for new events
        msg = consumer.poll(1.0)  # Provide a timeout in seconds

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print('Reached end of partition')
            else:
                print(f'Error: {msg.error().str()}')
        else:
            # Process the received message
            print(f'Received message: {msg.value().decode("utf-8")}')
            json = json.loads(msg.decode('utf-8'))  # maybe u have to change it

            src_vertex = json["src"]  # 324.234.342
            dst_vertex = json["dst"]  # "pp_account_2345"
            timestamp = json["timestamp"]  # empty
            event_type = json["event_type"]  # empty
            cassandra_session.execute(prepare_statement_container[event_type], (src_vertex, dst_vertex, timestamp))


except KeyboardInterrupt:
    # Close the consumer on keyboard interrupt
    consumer.close()


