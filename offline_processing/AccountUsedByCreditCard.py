from abc import ABC
from pyspark.shell import spark
from offline_processing.DataEnricherBase import DataEnricherBase


class AccountUsedByCreditCard(DataEnricherBase, ABC):

    def get_relevant_events_list(self):
        return ["credit_card_added_event"]

    def join_by_expression(self, partitioned_df, enricher_df):
        return partitioned_df[f"{self.get_src_column_name()}"] == enricher_df['account_number']

    def get_enriched_table(self):
        return spark.table("credit_card_info")

    def get_relevant_enriched_colums(self):
        return ["credit_card_number",
                "credit_limit",
                "expiration_date",
                "cardholder_name",
                "account_number"]

    def get_src_column_name(self):
        return "account_id"

    def get_src_type_column_name(self):
        return "ACCOUNT"

    def get_dst_column_name(self):
        return "credit_card_number"

    def get_dst_type_column_name(self):
        return "CREDIT_CARD"

    def get_timestamp_column_name(self):
        return "timestamp"

    def get_edge_type_name(self):
        return "USED_BY"
