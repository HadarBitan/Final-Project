from features_to_db import metric_container
from features_to_db.kafka_consumer import kafka_consumer
from features_to_db.props_extractor import cassandra_client


def enrich_data_with_location(edge_type, data):
    if edge_type == "account_ip_location":
        # Extract account and IP information from data
        account_id = data["dst"]
        ip_address = data["src"]

        # Get the location for the given IP address from the IP table
        location = get_location_by_ip(ip_address)

        # Perform data enrichment based on the account-IP-location relationship

        # Return the enriched data
        return enriched_data
    else:
        # Handle unsupported edge types or return default data
        return data


def get_location_by_ip(ip_address):
    CassandraClient = cassandra_client()
    query = f"SELECT location FROM {metric_container.features_to_tables['account_ip_location']} " \
            f"WHERE ip_address = '{ip_address}'"
    result = cassandra_client.execute_query(query)

    # Extract the location from the query result
    location = result["location"]

    return location


# Set up Kafka consumer
kafkaConsumer = kafka_consumer()

# Read messages from Kafka and write to Cassandra
for message in kafka_consumer.kafka_consumer:
    # Process each Kafka message and write to Cassandra
    enriched_data = enrich_data_with_location(message.edge_type, message.data)
    cassandra_client.insert_data(enriched_data)
