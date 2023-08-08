from abc import ABC

from pyspark.shell import spark

from offline_processing.DataEnricherBase import DataEnricherBase


class PhoneUsedByAccount(DataEnricherBase, ABC):

    def get_relevant_events_list(self):
        return ["PhoneUpdatedEvent"]

    def join_by_expression(self):
        return f"{self.get_src_column_name()} = phone_info['phone']"

    def get_enriched_table(self):
        return spark.table("edw.phone_info")

    def get_relevant_enriched_columns(self):
        return [
            "phone_created_timestamp",
            "phone_last_used",
            "phone_owner_name",
            "phone_number",
            "phone_type"
        ]

    def get_src_column_name(self):
        return "Phone"

    def get_src_type_column_name(self):
        return "Phone"

    def get_dst_column_name(self):
        return "pp_account"

    def get_dst_type_column_name(self):
        return "ACCOUNT"

    def get_timestamp_column_name(self):
        return "phone_created_timestamp"

    def get_edge_type_name(self):
        return "USED_BY"
