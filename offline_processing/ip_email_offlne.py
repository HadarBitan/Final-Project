from features_to_db import metric_container
from features_to_db.kafka_consumer import kafka_consumer
from features_to_db.props_extractor import cassandra_client


def enrich_data_with_email_ip_relationship(edge_type, data):
    """
    Enriches the data with email-IP relationship information.

    Args:
        edge_type (str): The type of edge representing the relationship.
        data (dict): The data to be enriched.

    Returns:
        dict: The enriched data.
    """
    if edge_type == "email_ip_relationship":
        # Extract email and IP information from data
        email = data["email"]
        ip_address = data["ip_address"]

        # Check if there is a relationship between the email and IP in the account_email_ip table
        has_relationship = check_email_ip_relationship(email, ip_address)

        # Perform data enrichment based on the email-IP relationship
        if has_relationship:
            # Extract additional information from the account_email_ip table
            query = f"SELECT additional_info FROM {metric_container.features_to_tables['account_email_ip']} " \
                    f"WHERE email = '{email}' AND ip_address = '{ip_address}'"
            result = cassandra_client.execute_query(query)

            # Get the additional information from the query result
            additional_info = result.get("additional_info", None)

            # Update the data with the additional information
            if additional_info:
                data["additional_info"] = additional_info

    return data


def check_email_ip_relationship(email, ip_address):
    """
    Checks if there is a relationship between the email and IP in the account_email_ip table.

    Args:
        email (str): The email address.
        ip_address (str): The IP address.

    Returns:
        bool: True if there is a relationship, False otherwise.
    """
    query = f"SELECT * FROM {metric_container.features_to_tables['account_email_ip']} " \
            f"WHERE email = '{email}' AND ip_address = '{ip_address}'"
    result = cassandra_client.execute_query(query)

    # Check if there is a relationship between the email and IP
    has_relationship = len(result) > 0

    return has_relationship


# Set up Kafka consumer
kafkaConsumer = kafka_consumer()

# Read messages from Kafka and write to Cassandra
for message in kafkaConsumer.kafka_consumer:
    # Process each Kafka message and write to Cassandra
    enriched_data = enrich_data_with_email_ip_relationship(message.edge_type, message.data)
    cassandra_client.insert_data(enriched_data)
