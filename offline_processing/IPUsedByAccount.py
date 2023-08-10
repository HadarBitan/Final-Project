from abc import ABC
from pyspark.shell import spark
from offline_processing.DataEnricherBase import DataEnricherBase


class IPAccount(DataEnricherBase, ABC):

    def get_relevant_events_list(self):
        return ["IPUpdatedEvent"]

    def join_by_expression(self):
        return "${self.get_src_column_name()} = IP_info['IP')"

    def get_enriched_table(self):
        return spark.table("edw.IP_info")

    def get_relevant_enriched_columns(self):
        return ["IP_created_timestamp",
                "IP_last_used",
                "IP_owner_name",
                "IP_location"]

    def get_src_column_name(self):
        return "IP"

    def get_src_type_column_name(self):
        return "IP"

    def get_dst_column_name(self):
        return "pp_account"

    def get_dst_type_column_name(self):
        return "ACCOUNT"

    def get_timestamp_column_name(self):
        return "IP_created_timestamp"

    def get_edge_type_name(self):
        return "USED_BY"
