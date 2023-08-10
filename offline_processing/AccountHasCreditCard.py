from abc import ABC
from pyspark.shell import spark
from offline_processing.DataEnricherBase import DataEnricherBase


class AccountToCreditCard(DataEnricherBase, ABC):

    def get_relevant_events_list(self):
        return ["CreditCardAddedEvent"]

    def join_by_expression(self):
        return f"{self.get_src_column_name()} = credit_card_info['account_number']"

    def get_enriched_table(self):
        return spark.table("edw.credit_card_info")

    def get_relevant_enriched_columns(self):
        return ["credit_card_number",
                "credit_limit",
                "expiration_date",
                "cardholder_name"]

    def get_src_column_name(self):
        return "account_number"

    def get_src_type_column_name(self):
        return "ACCOUNT"

    def get_dst_column_name(self):
        return "credit_card"

    def get_dst_type_column_name(self):
        return "CREDIT_CARD"

    def get_timestamp_column_name(self):
        return "credit_card_added_timestamp"

    def get_edge_type_name(self):
        return "HAS"
