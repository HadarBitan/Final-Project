from offline_processing.DataEnricherBase import DataEnricherBase


class LargeTransfersEnricher(DataEnricherBase):
    """
    Data enrichment class for identifying large transfers.

    Inherits from DataEnricherBase.
    """

    def enrich_data(self, row):
        """
        Enrich the transaction data for each row by identifying large transfers.

        Args:
            row (Row): A row in the DataFrame to be enriched.

        Returns:
            dict: The enriched data.
        """
        if row["transaction_amount"] > 100000:  # Define a threshold for large transfers
            row["is_large_transfer"] = True
        else:
            row["is_large_transfer"] = False
        return row

    def process_events(self, kafka_topic):
        """
        Process events from the Kafka topic and perform data enrichment for large transfers.

        Args:
            kafka_topic (str): The name of the Kafka topic containing the events.
        """
        # Read data from Kafka topic and create DataFrame
        df_kafka = self.read_kafka_topic_as_dataframe(kafka_topic)

        # Enrich the data with the specified DataFrame
        enriched_data = df_kafka.rdd.map(self.enrich_data).toDF()

        # Write the enriched data to the specified table
        enriched_data.write.format("json").mode("append").save("/tmp/large_transfers_enrichment")
