from abc import ABC
from pyspark.shell import spark
from offline_processing.DataEnricherBase import DataEnricherBase


class IPUsedByAccount(DataEnricherBase, ABC):

    def get_relevant_events_list(self):
        return ["IP_updated_event"]

    # def join_by_expression(self, partitioned_df, enricher_df):
    #     return partitioned_df[f"{self.get_src_column_name()}"] == enricher_df['IP_event']

    def join_by_expression(self, partitioned_df, enricher_df):
        return partitioned_df[self.get_src_column_name()] == enricher_df['IP_event']

    def get_enriched_table(self):
        return spark.table("IP_info")

    def get_relevant_enriched_colums(self):
        return ["IP_event",
                "IP_created_timestamp",
                "IP_last_used",
                "IP_owner_name",
                "IP_location",
                ]

    def get_src_column_name(self):
        return "IP"

    def get_src_type_column_name(self):
        return "IP"

    def get_dst_column_name(self):
        return "account_ID"

    def get_dst_type_column_name(self):
        return "ACCOUNT"

    def get_timestamp_column_name(self):
        return "IP_created_timestamp"

    def get_edge_type_name(self):
        return "USED_BY"
