import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import os
os.environ['PYSPARK_PYTHON'] = 'python'
import unittest

from offline_processing.IPUsedByAccount import IPUsedByAccount



class TestIpEdge(unittest.TestCase):

    def clean_dirty_test(self, spark):
        if spark.catalog.tableExists("IP_info_proccesed"):
            # Drop the existing table
            spark.sql("DROP TABLE IF EXISTS IP_info_proccesed")
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
            ("192.168.1.1", "2023-08-12 10:15:00", "2023-08-12 14:30:00", "John Doe", "New York", "1t", "12", "08",
             "2023"),
            (
                "192.168.1.2", "2023-08-12 12:30:00", "2023-08-12 18:45:00", "Jane Smith", "Los Angeles", "15", "12",
                "08",
                "2023"),
            (
            "192.168.1.3", "2023-08-12 08:00:00", "2023-08-12 16:00:00", "Michael Johnson", "Chicago", "15", "12", "08",
            "2023"),
            # Add more rows here
        ]

        # Create a DataFrame from the sample data

        schema = StructType([ \
            StructField("IP", StringType(), True), \
            StructField("IP_created_timestamp", StringType(), True), \
            StructField("IP_last_used", StringType(), True), \
            StructField("IP_owner_name", StringType(), True), \
            StructField("IP_location", StringType(), True), \
            StructField("hour", StringType(), True), \
            StructField("day", StringType(), True), \
            StructField("month", StringType(), True), \
            StructField("year", StringType(), True) \
 \
            ])

        data_info_enrich = [
            ("123", "192.168.1.1", "Desktop", "Windows 10", "Login", "2023-08-12 18:45:00", "2023-08-12 21:30:00"),
            ("456", "192.168.1.2", "Mobile", "iOS 14", "Logout", "2023-08-12 13:30:00", "2023-08-12 15:15:00"),
            ("789", "192.168.1.3", "Laptop", "Ubuntu 20.04", "Login", "2023-08-12 15:30:00", "2023-08-12 17:45:00"),
            # Add more rows here
        ]

        # Create a DataFrame from the sample data
        schema_info = StructType([ \
            StructField("account_ID", StringType(), True),
            StructField("IP_event", StringType(), True),
            StructField("Device_Type", StringType(), True), \
            StructField("Operating_System", StringType(), True), \
            StructField("Action_Type", StringType(), True), \
            StructField("Login_Time", StringType(), True), \
            StructField("Logout_Time", StringType(), True), \
 \
            ])



        df = spark.createDataFrame(data, schema)
        df_info = spark.createDataFrame(data_info_enrich, schema_info)
        df.createTempView("IP_updated_event_partitioned")
        df_info.createTempView("ip_info")
    def test_ip_used_by_account(self):

        my_datetime = datetime(2023, 8, 12, 15, 30, 0)  # Year, Month, Day, Hour, Minute, Second

        spark = SparkSession.builder \
            .appName("DataEnricher1") \
            .master("local[*]").getOrCreate()

        self.clean_dirty_test(spark)
        self.preper_data(spark)

        ip_used_by_account = IPUsedByAccount(spark)

        ip_used_by_account.join_kafka_with_table(my_datetime, "ip_updated_event")
        spark.table("ip_updated_event_proccesed").show()
        assert spark.table("ip_updated_event_proccesed").count() == 2
        spark.stop()


if __name__ == '__main__':
    unittest.main()
