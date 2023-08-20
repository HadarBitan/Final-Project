from abc import ABC

from pyspark.shell import spark

from offline_processing.DataEnricherBase import DataEnricherBase


class AccountTransactionAccount(DataEnricherBase, ABC):

    def get_relevant_events_list(self):
        return ["account_transfer_account_event"]

    def join_by_expression(self, partitioned_df, enricher_df):
        return partitioned_df[f"{self.get_src_column_name()}"] == enricher_df["source_account_event"]

    def get_enriched_table(self):
        return spark.table("account_transfer_account_info")

    def get_relevant_enriched_colums(self):
        return [
            "source_account",
            "number_of_transfer",
            "destination_account",
            "transaction_timestamp",
            "transaction_amount",
            "transaction_type",
            "transaction_status",
            "transaction_description",
            "transaction_fee",
            "transaction_currency",
        ]

    def get_src_column_name(self):
        return "source_account"

    def get_src_type_column_name(self):
        return "ACCOUNT"

    def get_dst_column_name(self):
        return "destination_account"

    def get_dst_type_column_name(self):
        return "ACCOUNT"

    def get_timestamp_column_name(self):
        return "transaction_timestamp"

    def get_edge_type_name(self):
        return "TRANSFER"
