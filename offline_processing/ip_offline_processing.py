from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json
from pyspark.sql.types import StringType, StructType

from features_to_db.props_extractor import cassandra_client
from features_to_db import metric_container
from offline_processing.sparkReadFromKafka import spark_read_from_kafka
from offline_processing.sparkWriteToKafke import kafka_writer


def enrich_data_with_location(edge_type, data):
    if edge_type == "account_ip_location":
        # Extract account and IP information from data
        account_id = data["dst"]
        ip_address = data["src"]

        # Get the location for the given IP address from the IP table
        location = get_location_by_ip(ip_address)

        # Perform data enrichment based on the account-IP-location relationship
        data["location"] = location

    return data


def get_location_by_ip(ip_address):
    cassandra_client()
    query = f"SELECT location FROM {metric_container.features_to_tables['account_ip_location']} " \
            f"WHERE ip_address = '{ip_address}'"
    result = cassandra_client.execute_query(query)

    # Extract the location from the query result
    location = result["location"]

    return location


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
enriched_data = df.rdd.map(lambda row: enrich_data_with_location(row.edge_type, row.data))

# Convert the enriched data back to DataFrame
enriched_df = enriched_data.toDF()

# Serialize the enriched data back to JSON format
enriched_df = enriched_df.select(to_json(enriched_df).alias("value"))

# Write enriched data to Kafka using Spark
kafka_writer(enriched_df)

# Stop the Spark session
spark.stop()
