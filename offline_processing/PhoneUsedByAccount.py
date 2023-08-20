from abc import ABC

from pyspark.shell import spark

from offline_processing.DataEnricherBase import DataEnricherBase


class PhoneUsedByAccount(DataEnricherBase, ABC):

    def get_relevant_events_list(self):
        return ["phone_updated_event"]

    def join_by_expression(self, partitioned_df, enricher_df):
        return partitioned_df[f"{self.get_src_column_name()}"] == enricher_df['phone']

    def get_enriched_table(self):
        return spark.table("phone_info")

    def get_relevant_enriched_colums(self):
        return [
            "phone_created_timestamp",
            "account_id",
            "phone_number",
            "phone_type",
            "phone_last_used",
            "phone_owner_name",
        ]

    def get_src_column_name(self):
        return "phone_number"

    def get_src_type_column_name(self):
        return "PHONE"

    def get_dst_column_name(self):
        return "account_id"

    def get_dst_type_column_name(self):
        return "ACCOUNT"

    def get_timestamp_column_name(self):
        return "phone_created_timestamp"

    def get_edge_type_name(self):
        return "USED_BY"
