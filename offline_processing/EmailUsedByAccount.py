from abc import ABC

from pyspark.shell import spark

from offline_processing.DataEnricherBase import DataEnricherBase


from abc import ABC
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from kafka import KafkaConsumer
import json


class EmailUsedByAccount(DataEnricherBase, ABC):

    def get_relevant_events_list(self):
        return ["EmailUpdatedEvent"]

    def join_by_expression(self):
        return f"{self.get_src_column_name()} = email_info['org_email']"

    def get_enriched_table(self):
        return spark.table("edw.email_info")

    def get_relevant_enriched_colums(self):
        return ["email_created_timestmap", "email_last_used", "backup_email", "email_owner_name"]
        
    def get_src_column_name(self):
        return "email"
        
    def get_src_type_column_name(self):
        return "EMAIL"
        
    def get_dst_column_name(self):
        return "pp_account"    

    def get_dst_type_column_name(self):
        return "ACCOUNT"  
        
    def get_timestamp_column_name(self):
        return "email_created_timestamp"

    def get_edge_type_name(self):
        return "USED_BY"   

  
