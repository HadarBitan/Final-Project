from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from kafka import KafkaConsumer
import json
from offline_processing.DataEnricherBase import DataEnricherBase


class TransactionEnricher(DataEnricherBase):
    """
    Data enrichment class for enriching transaction data.

    Inherits from DataEnricherBase.
    """

    def enrich_data(self, row):
        """
        Enrich the transaction data for each row.

        Args:
            row (Row): A row in the DataFrame to be enriched.

        Returns:
            dict: The enriched data.
        """
        # Assuming the DataFrame has columns: "account_id", "amount", "transaction_time", and "ip_address"
        account_id = row["account_id"]
        amount = row["amount"]
        transaction_time = row["transaction_time"]
        source_ip = row["source_ip"]
        destination_ip = row["destination_ip"]

        # Read data from the existing DataFrame (account_info table)
        account_info_df = self.spark.table("account_info")

        # Filter the DataFrame to find the account's information
        account_data = account_info_df.filter(account_info_df.account_id == account_id) \
            .select("account_id", "balance", "monthly_transactions", "max_transaction_amount", "max_transaction_date",
                    "ip_address") \
            .collect()[0]

        # Update the data with account's information
        row["balance"] = account_data["balance"]
        row["monthly_transactions"] = account_data["monthly_transactions"]
        row["max_transaction_amount"] = account_data["max_transaction_amount"]
        row["max_transaction_date"] = account_data["max_transaction_date"]
        row["ip_address"] = account_data["ip_address"]

        return row

    def process_transactions(self, kafka_topic):
        """
        Process transaction events from the Kafka topic and perform data enrichment.

        Args:
            kafka_topic (str): The name of the Kafka topic containing the transaction events.
        """
        # Read data from Kafka topic and create DataFrame
        df_kafka = self.read_kafka_topic_as_dataframe(kafka_topic)

        # Perform the JOIN on the specified condition
        join_condition = df_kafka["account_id"] == col("account_info.account_id")
        joined_df = df_kafka.join("account_info", join_condition, "inner")

        # Enrich the data with the specified DataFrame
        enriched_data = joined_df.rdd.map(self.enrich_data).toDF()

        # Write the enriched data to Kafka topic "enriched_transactions"
        enriched_data.write.format("json").mode("append").save("/tmp/enriched_transactions")
