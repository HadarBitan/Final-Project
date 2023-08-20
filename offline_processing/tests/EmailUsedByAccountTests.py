import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import os

os.environ['PYSPARK_PYTHON'] = 'python'
from offline_processing.EmailUsedByAccount import EmailUsedByAccount


import unittest


class TestEmailEdge(unittest.TestCase):

    def clean_dirty_test(self, spark):
        os.environ['PYSPARK_PYTHON'] = 'python'
        if spark.catalog.tableExists("email_changed_event_proccesed"):
            # Drop the existing table
            spark.sql("DROP TABLE IF EXISTS email_changed_event_proccesed")
        directory_path = "C:/Users/Tal%20Ornan/PycharmProjects/Final-Project/offline_processing/tests/spark-warehouse"
        if os.path.exists(directory_path):
            try:
                shutil.rmtree(directory_path)
                print(f"Directory '{directory_path}' successfully deleted.")
            except Exception as e:
                print(f"Error deleting directory '{directory_path}': {str(e)}")
        else:
            print(f"Directory '{directory_path}' does not exist.")

    def preper_data(self, spark):
        data = [
            ("2023-08-12 15:30:00", "user@example.com", "account123", "15", "12", "08", "2023"),
            ("2023-08-12 16:45:00", "user2@example.com", "account456", "15", "12", "08", "2023")
            # Add more rows here
        ]

        # Create a DataFrame from the sample data

        schema = StructType([ \
            StructField("timestamp", StringType(), True), \
            StructField("email", StringType(), True), \
            StructField("pp_account", StringType(), True), \
            StructField("hour", StringType(), True), \
            StructField("day", StringType(), True), \
            StructField("month", StringType(), True), \
            StructField("year", StringType(), True) \
 \
            ])

        data_info_enrich = [
            ("user@example.com", "email_created_timestmap", "email_last_used", "backup_email", "email_owner_name"),
            ("user2@example.com", "email_created_timestmap", "email_last_used", "backup_email", "email_owner_name")
            # Add more rows here
        ]

        # Create a DataFrame from the sample data

        schema_info = StructType([ \
            StructField("org_email", StringType(), True),
            StructField("email_created_timestmap", StringType(), True), \
            StructField("email_last_used", StringType(), True), \
            StructField("backup_email", StringType(), True), \
            StructField("email_owner_name", StringType(), True) \
            ])
        df = spark.createDataFrame(data, schema)
        df_info = spark.createDataFrame(data_info_enrich, schema_info)
        # df_info.write.mode("overwrite").saveAsTable("email_info")

        df.createTempView("email_changed_event_partitioned")
        df_info.createTempView("email_info")

    def test_email_used_by_account(self):

        my_datetime = datetime(2023, 8, 12, 15, 30, 0)  # Year, Month, Day, Hour, Minute, Second

        spark = SparkSession.builder \
            .appName("DataEnricher1") \
            .master("local[*]").getOrCreate()

        self.clean_dirty_test(spark)
        self.preper_data(spark)

        email_by_pp_account = EmailUsedByAccount(spark)

        email_by_pp_account.join_kafka_with_table(my_datetime, "email_changed_event")
        spark.table("email_changed_event_proccesed").show()
        assert spark.table("email_changed_event_proccesed").count() == 2
        spark.stop()


if __name__ == '__main__':
    unittest.main()