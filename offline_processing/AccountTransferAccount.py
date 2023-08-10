from abc import ABC

from pyspark.shell import spark

from offline_processing.DataEnricherBase import DataEnricherBase


class AccountTransactionAccount(DataEnricherBase, ABC):

    def get_relevant_events_list(self):
        return ["TransactionEvent"]

    def join_by_expression(self):
        return f"{self.get_src_column_name()} = transactions['source_account'] " \
               f"OR {self.get_dst_column_name()} = transactions['destination_account']"

    def get_enriched_table(self):
        return spark.table("edw.transactions")

    def get_relevant_enriched_columns(self):
        return [
            "transaction_amount",
            "transaction_timestamp",
            "transaction_type",
            "transaction_status",
            "transaction_description",
            "transaction_fee",
            "transaction_currency",
            "transaction_location"
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
