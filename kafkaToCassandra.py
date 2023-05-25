from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Set up Kafka consumer
kafka_consumer = KafkaConsumer(
    'student-start',
    bootstrap_servers='localhost:9092',
    group_id='first_kafka_cassandra_consume'
)#we choose the group_id name

# Set up Cassandra connection
cassandra_cluster = Cluster(['localhost'])
cassandra_session = cassandra_cluster.connect('student_start')

# Read messages from Kafka and write to Cassandra
for message in kafka_consumer:
    # Extract the necessary data from Kafka message
    student_data = message.value.decode('utf-8').split(',')

    # Convert the id value to an integer
    id_value = int(student_data[0])

    # Prepare the query to insert data into Cassandra table
    query = "INSERT INTO students (id, name, major) VALUES (?, ?, ?)"
    prepared_query = cassandra_session.prepare(query)

    # Execute the query
    cassandra_session.execute(prepared_query, (id_value, student_data[1], student_data[2]))
