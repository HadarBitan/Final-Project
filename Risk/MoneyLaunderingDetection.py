from offline_processing.DataEnricherBase import DataEnricherBase


class AntiMoneyLaunderingEnrichment(DataEnricherBase):
    def enrich_data(self, row):
        """
        Enriches the data with potential money laundering identification.

        This method checks whether the current transaction might indicate potential money laundering
        by examining if there are previous transactions with the same source account and amount.

        Args:
            row (dict): A dictionary containing transaction data.

        Returns:
            dict: The enriched data with an additional field indicating potential money laundering.
        """
        source_account = row["source_account"]
        amount = row["amount"]
        transaction_time = row["transaction_time"]

        # Read data from the existing DataFrame (transactions table)
        transactions_df = self.spark.table("transactions")

        # Filter the DataFrame to find transactions with the same source account and amount
        potential_money_laundering = transactions_df.filter(
            (transactions_df.source_account == source_account) &
            (transactions_df.amount == amount) &
            (transactions_df.transaction_time != transaction_time)
        )

        # If potential_money_laundering is not empty, it means there's a previous transaction with the same
        # source account and amount, which might indicate money laundering
        if potential_money_laundering.count() > 0:
            row["potential_money_laundering"] = True
        else:
            row["potential_money_laundering"] = False

        return row

    def process_transactions(self, kafka_topic):
        """
        Process transactions from the Kafka topic and perform potential money laundering identification.

        Args:
            kafka_topic (str): The name of the Kafka topic containing the transaction events.
        """
        # Read data from Kafka topic and create DataFrame
        df_kafka = self.read_kafka_topic_as_dataframe(kafka_topic)

        # Perform the enrichment using the defined enrich_data method
        enriched_data = df_kafka.rdd.map(self.enrich_data).toDF()

        # Write the enriched data to a new Kafka topic named "enriched_transactions"
        enriched_data.selectExpr("CAST(source_account AS STRING) AS key", "to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "enriched_transactions") \
            .save()
