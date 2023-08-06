from offline_processing.DataEnricherBase import DataEnricherBase


class UnusualHourTransfersEnricher(DataEnricherBase):
    """
    Data enrichment class for identifying unusual hour transfers.

    Inherits from DataEnricherBase.
    """

    def enrich_data(self, row):
        """
        Enrich the transaction data for each row by identifying unusual hour transfers.

        Args:
            row (Row): A row in the DataFrame to be enriched.

        Returns:
            dict: The enriched data.
        """
        transaction_time = row["event_time"]
        hour = transaction_time.hour

        if hour < 6 or hour > 22:  # Define the range of "normal" hours
            row["unusual_hour_transfer"] = True
        else:
            row["unusual_hour_transfer"] = False
        return row

    def process_events(self, kafka_topic):
        """
        Process events from the Kafka topic and perform data enrichment for unusual hour transfers.

        Args:
            kafka_topic (str): The name of the Kafka topic containing the events.
        """
        # Read data from Kafka topic and create DataFrame
        df_kafka = self.read_kafka_topic_as_dataframe(kafka_topic)

        # Enrich the data with the specified DataFrame
        enriched_data = df_kafka.rdd.map(self.enrich_data).toDF()

        # Write the enriched data to the specified table
        enriched_data.write.format("json").mode("append").save("/tmp/unusual_hour_transfers_enrichment")
