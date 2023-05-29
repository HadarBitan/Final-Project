from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Set up Cassandra connection
from features_to_db import kafka_consumer, metric_container, props_extractor

cassandra_cluster = Cluster([props_extractor])
cassandra_session = cassandra_cluster.connect('Final_Project')
query = "INSERT INTO 'offline_graph' (src, dst, edge_value, props, src_type, dst_type, edge_type) VALUES (?, ?, ?, ?, ?, ?, ?)"

prepared_query = cassandra_session.prepare(query)
prepare_statement_container = {}
for name,table in metric_container.features_to_tables.items():
    prepare_statement_container[name] = cassandra_session.prepare(query.format(table))
###

# Read messages from Kafka and write to Cassandra
for message in kafka_consumer.kafka_consumer:
    # Extract the necessary data from Kafka message
    json = message.value.decode('utf-8').split(',') # maybe u have to change it

    src_vertex = json["src"] #324.234.342
    dst_vertex = json["dst"] #"pp_account_2345"
    edge_value = json["value"] #empty
    edge_props = json["props"] # it should be of type map, something like {"os":"window", "time":324234, "browser": "chrome"}
    src_type = json["src_type"] #"ip
    dst_type = json["dst_type"] #"account"
    edge_type = json["edge_Type"] #"used_By"
    mode = json["mode"] #online

    full_type = "{0}_{1}_{2}_{3}".format(src_type, edge_type, dst_type, mode)
    # Prepare the query to insert data into Cassandra table

    # Execute the query
    cassandra_session.execute(prepare_statement_container[full_type], (src_vertex,dst_vertex,
                                                                      edge_value, edge_props, src_type, dst_type, edge_type))
