from abc import ABC

from pyspark.shell import spark
from offline_processing.DataEnricherBase import DataEnricherBase
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from abc import ABC


class EmailUsedByAccount(DataEnricherBase, ABC):

    def get_relevant_events_list(self):
        return ["EmailUpdatedEvent"]

    def join_by_expression(self, partitioned_df, enricher_df):
        return partitioned_df[f"{self.get_src_column_name()}"] == enricher_df['org_email']

    def get_enriched_table(self):
        return spark.table("email_info")

    def get_relevant_enriched_colums(self):
        return ["email_info.email_created_timestmap", "email_info.email_last_used", "email_info.backup_email",
                "email_info.email_owner_name"]
        
    def get_src_column_name(self):
        return "email"
        
    def get_src_type_column_name(self):
        return "EMAIL"
        
    def get_dst_column_name(self):
        return "pp_account"    

    def get_dst_type_column_name(self):
        return "ACCOUNT"  
        
    def get_timestamp_column_name(self):
        return "timestamp"

    def get_edge_type_name(self):
        return "USED_BY"   

  
