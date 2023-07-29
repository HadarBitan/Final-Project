from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from kafka import KafkaConsumer
import json

# Create a Spark session
spark = SparkSession.builder \
    .appName("Email_Account_Edges") \
    .getOrCreate()

# Create a Kafka consumer
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'email_changes_topic'
consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_bootstrap_servers)


# Function to enrich data with email and account information
def enrich_data_with_email_account_relationship(email, account_id, data):
    # Read data from the existing DataFrame (Emails table)
    emails_df = spark.read.format("delta").load("/tmp/emails")

    # Filter the DataFrame to find the email's creation time and last activity time
    email_info = emails_df.filter(emails_df.email == email) \
        .select("email", "creation_time", "last_activity_time") \
        .collect()[0]

    # Update the data with email's creation time and last activity time
    data["email_creation_time"] = email_info["creation_time"]
    data["email_last_activity_time"] = email_info["last_activity_time"]

    return data

#talornan
# Process events from Kafka and generate edges
def process_events(events):
    for event in events:
        # Read data from the event
        event_data = json.loads(event.value.decode('utf-8'))
        email = event_data['email']
        account_id = event_data['account_id']
        event_time = event_data['time']

        # Enrich the event data with email and account information
        enriched_data = enrich_data_with_email_account_relationship(email, account_id, event_data)

        # Create a DataFrame for the enriched data
        enriched_df = spark.createDataFrame([enriched_data],
                                            ["email", "account_id", "event_time", "email_creation_time",
                                             "email_last_activity_time"])

        # Write the enriched data to a new Delta table (email_account_relation)
        enriched_df.write.format("delta").mode("append").save("/tmp/email_account_relation")

        # Print the enriched data (for demonstration purposes)
        print(enriched_data)


# Start processing events from Kafka
process_events(consumer)

# Stop the Spark session
spark.stop()
