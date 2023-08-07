from datetime import timedelta

from offline_processing.DataEnricherBase import DataEnricherBase


class HighFrequencyTransfersEnricher(DataEnricherBase):
    """
    Data enrichment class for identifying high-frequency transfers.

    Inherits from DataEnricherBase.
    """

    def enrich_data(self, row):
        """
        Enrich the transaction data for each row by identifying high-frequency transfers.

        Args:
            row (Row): A row in the DataFrame to be enriched.

        Returns:
            dict: The enriched data.
        """
        account_id = row["account_id"]
        transaction_time = row["event_time"]

        # Read data from the existing DataFrame (Transfers table)
        transfers_df = self.spark.table("transfers_info")

        # Count the number of transfers from the same account within a 1-hour window
        high_frequency_transfers = transfers_df.filter((transfers_df.account_id == account_id) &
                                                       (transfers_df.event_time >= transaction_time - timedelta(
                                                           hours=1)) &
                                                       (transfers_df.event_time <= transaction_time)).count()

        row["high_frequency_transfers"] = high_frequency_transfers
        return row

    def process_events(self, kafka_topic):
        """
        Process events from the Kafka topic and perform data enrichment for high-frequency transfers.

        Args:
            kafka_topic (str): The name of the Kafka topic containing the events.
        """
        # Read data from Kafka topic and create DataFrame
        df_kafka = self.read_kafka_topic_as_dataframe(kafka_topic)

        # Enrich the data with the specified DataFrame
        enriched_data = df_kafka.rdd.map(self.enrich_data).toDF()

        # Write the enriched data to the specified table
        enriched_data.write.format("json").mode("append").save("/tmp/high_frequency_transfers_enrichment")
