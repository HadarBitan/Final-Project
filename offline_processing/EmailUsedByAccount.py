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

  
my_datetime = datetime(2023, 8, 12, 15, 30, 0)  # Year, Month, Day, Hour, Minute, Second

data = [
    ("2023-08-12 15:30:00", "user@example.com", "account123", "15", "12", "08", "2023"),
    ("2023-08-12 16:45:00", "user2@example.com", "account456", "15", "12", "08", "2023")
    # Add more rows here
]

# Create a DataFrame from the sample data


schema = StructType([ \
    StructField("timestamp",StringType(),True), \
    StructField("email",StringType(),True), \
    StructField("pp_account",StringType(),True), \
    StructField("hour", StringType(), True), \
    StructField("day", StringType(), True), \
    StructField("month", StringType(), True), \
    StructField("year", StringType(), True) \
 \
    ])



data_info_enrich = [
    ("user@example.com","email_created_timestmap","email_last_used","backup_email", "email_owner_name"),
    ("user2@example.com","email_created_timestmap","email_last_used","backup_email", "email_owner_name")
    # Add more rows here
]

# Create a DataFrame from the sample data


schema_info = StructType([ \
    StructField("org_email",StringType(),True),
    StructField("email_created_timestmap", StringType(), True), \
    StructField("email_last_used", StringType(), True), \
    StructField("backup_email", StringType(), True), \
    StructField("email_owner_name", StringType(), True) \
    ])

import os

os.environ['PYSPARK_PYTHON'] = 'python'

spark = SparkSession.builder \
    .appName("DataEnricher1") \
    .master("local[*]").getOrCreate()
spark.stop()
spark = SparkSession.builder \
    .appName("DataEnricher1") \
    .master("local[*]").getOrCreate()
df = spark.createDataFrame(data, schema)
df_info = spark.createDataFrame(data_info_enrich, schema_info)

df.createTempView("email_changed_event_partitioned")
df_info.createTempView("email_info")

email_by_pp_account = EmailUsedByAccount(spark)

email_by_pp_account.join_kafka_with_table(my_datetime, "email_changed_event")
spark.table("email_changed_event_proccesed").show()
