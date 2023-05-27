from cassandra.cluster import Cluster

# Cassandra's connection details
contact_points = ['localhost']
port = 9042
keyspace = 'student_start'

# Set up Cassandra cluster and session
cassandra_cluster = Cluster(contact_points=contact_points, port=port)
cassandra_session = cassandra_cluster.connect(keyspace)

# Execute a SELECT query to retrieve data from Cassandra
select_query = "SELECT id, name, major FROM students"
result_set = cassandra_session.execute(select_query)

# Process the retrieved data
for row in result_set:
    print(f"ID: {row.id}, Name: {row.name}, Major: {row.major}")

# Close the Cassandra session and cluster
cassandra_session.shutdown()
cassandra_cluster.shutdown()
