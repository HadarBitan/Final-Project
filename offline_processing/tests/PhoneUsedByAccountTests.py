import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import os

os.environ['PYSPARK_PYTHON'] = 'python'

import unittest

from offline_processing.PhoneUsedByAccount import PhoneUsedByAccount


class TestPhoneEdge(unittest.TestCase):

    def clean_dirty_test(self, spark):
        if spark.catalog.tableExists("phone_updated_event"):
            # Drop the existing table
            spark.sql("DROP TABLE IF EXISTS phone_updated_event_proccesed")
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
            ("2023-08-12 10:15:00", "1a2b3c4d", "555-123-4567", "Mobile", "2023-08-12 14:30:00", "John Doe", "15", "12",
             "08",
             "2023"),
            ("2023-08-12 12:30:00", "5e6f7g8h", "555-987-6543", "Home", "2023-08-12 18:45:00", "Jane Smith", "15", "12",
             "08",
             "2023")
            # Add more rows here
        ]

        # Create a DataFrame from the sample data

        schema = StructType([ \
            StructField("timestamp", StringType(), True), \
            StructField("account_id", StringType(), True), \
            StructField("phone_number", StringType(), True), \
            StructField("phone_type", StringType(), True), \
            StructField("phone_last_used", StringType(), True), \
            StructField("phone_owner_name", StringType(), True), \
            StructField("hour", StringType(), True), \
            StructField("day", StringType(), True), \
            StructField("month", StringType(), True), \
            StructField("year", StringType(), True) \
 \
            ])

        data_info_enrich = [
            ("555-987-6543", "2023-08-01 09:00:00", "555-555-5555", "2023-08-12 15:30:00",
             "2023-08-10 08:45:00",
             "2023-08-12 "
             "14:30:00"),
            ("555-987-6543", "2023-07-15 14:30:00", "555-999-8888", "2023-08-12 12:00:00",
             "2023-08-11 09:15:00",
             "2023-08-12 "
             "18:45:00")
            # Add more rows here
        ]

        # Create a DataFrame from the sample data

        schema_info = StructType([ \
            StructField("phone", StringType(), True),
            StructField("phone_created_timestamp", StringType(), True), \
            StructField("backup_phone_number", StringType(), True), \
            StructField("last_app_login_time", StringType(), True), \
            StructField("last_security_alert_time", StringType(), True), \
            StructField("last_purchase_time", StringType(), True) \
            ])

        df = spark.createDataFrame(data, schema)
        df_info = spark.createDataFrame(data_info_enrich, schema_info)
        df.createTempView("phone_updated_event_partitioned")
        df_info.createTempView("phone_info")
        df.show()
        df_info.show()

    def test_phone_used_by_account(self):

        my_datetime = datetime(2023, 8, 12, 15, 30, 0)  # Year, Month, Day, Hour, Minute, Second

        spark = SparkSession.builder \
            .appName("DataEnricher1") \
            .master("local[*]").getOrCreate()

        self.clean_dirty_test(spark)
        self.preper_data(spark)

        phone_usedBy_account = PhoneUsedByAccount(spark)

        phone_usedBy_account.join_kafka_with_table(my_datetime, "phone_updated_event")
        spark.table("phone_updated_event_proccesed").show()
        assert spark.table("phone_updated_event_proccesed").count() == 2
        spark.stop()


if __name__ == '__main__':
    unittest.main()
