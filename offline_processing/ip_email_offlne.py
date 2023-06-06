from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json
from pyspark.sql.types import StringType, StructType

from features_to_db.props_extractor import cassandra_client
from features_to_db import metric_container
from offline_processing.sparkReadFromKafka import spark_read_from_kafka
from offline_processing.sparkWriteToKafke import kafka_writer


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
            additional_info = get_additional_info(email, ip_address)

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
    cassandra_client()
    query = f"SELECT * FROM {metric_container.features_to_tables['account_email_ip']} " \
            f"WHERE email = '{email}' AND ip_address = '{ip_address}'"
    result = cassandra_client.execute_query(query)

    # Check if there is a relationship between the email and IP
    has_relationship = len(result) > 0

    return has_relationship


def get_additional_info(email, ip_address):
    """
    Retrieves additional information from the account_email_ip table.

    Args:
        email (str): The email address.
        ip_address (str): The IP address.

    Returns:
        str: The additional information.
    """
    cassandra_client()
    query = f"SELECT additional_info FROM {metric_container.features_to_tables['account_email_ip']} " \
            f"WHERE email = '{email}' AND ip_address = '{ip_address}'"
    result = cassandra_client.execute_query(query)

    additional_info = result.get("additional_info", None)

    return additional_info


# Set up Spark session
spark = SparkSession.builder \
    .appName("EnrichData") \
    .getOrCreate()

# Read messages from Kafka using Spark
data = spark_read_from_kafka(spark)

# Define the schema for the incoming messages
schema = StructType().add("edge_type", StringType()).add("data", StringType())

# Deserialize the data from Kafka into DataFrame using the schema
df = data.select(from_json(data.value, schema).alias("json")).select("json.*")

# Enrich the data
enriched_data = df.rdd.map(lambda row: enrich_data_with_email_ip_relationship(row.edge_type, row.data))

# Convert the enriched data back to DataFrame
enriched_df = enriched_data.toDF()

# Serialize the enriched data to JSON
enriched_df_json = enriched_df.select(to_json(enriched_df).alias("value"))

# Write enriched data to Kafka using Spark
kafka_writer(enriched_df_json)

# Stop Spark session
spark.stop()


