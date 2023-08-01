from offline_processing.DataEnricherBase import DataEnricherBase


class EmailAccount(DataEnricherBase):
    """
    Data enrichment class for enriching account-email data.

    Inherits from DataEnricherBase.
    """

    def enrich_data(self, row):
        """
        Enrich the account-email data for each row.

        Args:
            row (Row): A row in the DataFrame to be enriched.

        Returns:
            dict: The enriched data.
        """
        # Assuming the DataFrame has columns: "email", "account_id", "event_time", and "email_creation_time"
        email = row["email"]
        account_id = row["account_id"]
        event_time = row["event_time"]

        # Read data from the existing DataFrame (Emails table)
        emails_df = self.spark.table("email_info")

        # Filter the DataFrame to find the email's creation time and last activity time
        email_info = emails_df.filter(emails_df.email == email) \
            .select("email", "creation_time", "last_activity_time") \
            .collect()[0]

        # Update the data with email's creation time and last activity time
        row["email_creation_time"] = email_info["creation_time"]
        row["email_last_activity_time"] = email_info["last_activity_time"]

        return row

    def process_events(self, kafka_topic):
        """
        Process events from the Kafka topic and perform data enrichment.

        Args:
            kafka_topic (str): The name of the Kafka topic containing the events.
        """
        # Read data from Kafka topic and create DataFrame
        df_kafka = self.read_kafka_topic_as_dataframe(kafka_topic)

        # Perform the JOIN on the specified condition
        join_condition = df_kafka["account_id"] == "email_info.account_id"
        joined_df = df_kafka.join("email_info", join_condition, "inner")

        # Enrich the data with the specified DataFrame
        enriched_data = joined_df.rdd.map(self.enrich_data).toDF()

        # Write the enriched data to kafka table "emailAccountEnrichment"
        enriched_data.write.format("json").mode("append").save("/tmp/emailAccountEnrichment")
