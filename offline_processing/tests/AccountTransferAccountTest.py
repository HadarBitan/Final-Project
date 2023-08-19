import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import os

os.environ['PYSPARK_PYTHON'] = 'python'
from offline_processing.AccountTransferAccount import AccountTransactionAccount
import unittest

my_datetime = datetime(2023, 8, 12, 15, 30, 0)  # Year, Month, Day, Hour, Minute, Second

data = [
    ("account123", "1111", "account456", "2023-08-12 15:30:00", 5000, "transfer", "completed",
     "transfer between accounts",
     "0.5", "USD", "15", "12", "08", "2023"),
    ("account456", "2222", "account123", "2023-08-12 16:45:00", 2000, "debit", "pending", "payment for online purchase",
     "0.1",
     "EUR", "16", "12", "08", "2023")
    # Add more rows here
]

# Create a DataFrame from the sample data

schema = StructType([ \
    StructField("source_account", StringType(), True), \
    StructField("number_of_transfer", StringType(), True), \
    StructField("destination_account", StringType(), True), \
    StructField("transaction_timestamp", StringType(), True), \
    StructField("transaction_amount", IntegerType(), True), \
    StructField("transaction_type", StringType(), True), \
    StructField("transaction_status", StringType(), True), \
    StructField("transaction_description", StringType(), True), \
    StructField("transaction_fee", StringType(), True), \
    StructField("transaction_currency", StringType(), True), \
    StructField("hour", StringType(), True), \
    StructField("day", StringType(), True), \
    StructField("month", StringType(), True), \
    StructField("year", StringType(), True) \
 \
    ])

data_info_enrich = [
    ("account123", "omer@example.com", "123456789", 1000, 5000, "False"),
    ("account456", "tal@example.com", "987654321", 1500, 3000, "True")
    # Add more rows here
]

# Create a DataFrame from the sample data
schema_info = StructType([ \
    StructField("source_account_event", StringType(), True),
    StructField("Email", StringType(), True), \
    StructField("Phone Number", StringType(), True), \
    StructField("Weekly Transfer Amount", IntegerType(), True), \
    StructField("Transfer Limit", IntegerType(), True), \
    StructField("Blocked Status", StringType(), True), \
 \
    ])

spark = SparkSession.builder \
    .appName("DataEnricher3") \
    .master("local[*]").getOrCreate()

if spark.catalog.tableExists("account_transfer_account_event_proccesed"):
    # Drop the existing table
    spark.sql("DROP TABLE IF EXISTS account_transfer_account_event_proccesed")

df = spark.createDataFrame(data, schema)
df_info = spark.createDataFrame(data_info_enrich, schema_info)
# df_info.write.mode("overwrite").saveAsTable("email_info")

df.createTempView("account_transfer_account_event_partitioned")
df_info.createTempView("account_transfer_account_info")

account_transfer_account = AccountTransactionAccount(spark)

account_transfer_account.join_kafka_with_table(my_datetime, "account_transfer_account_event")
spark.table("account_transfer_account_event_proccesed").show()



class TestAccountTransferEdge(unittest.TestCase):

    def clean_dirty_test(self, spark):
        if spark.catalog.tableExists("account_transfer_account_event_proccesed"):
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
            ("account123", "1111", "account456", "2023-08-12 15:30:00", 5000, "transfer", "completed",
             "transfer between accounts",
             "0.5", "USD", "15", "12", "08", "2023"),
            ("account456", "2222", "account123", "2023-08-12 16:45:00", 2000, "debit", "pending",
             "payment for online purchase",
             "0.1",
             "EUR", "15", "12", "08", "2023")
            # Add more rows here
        ]

        # Create a DataFrame from the sample data

        schema = StructType([ \
            StructField("source_account", StringType(), True), \
            StructField("number_of_transfer", StringType(), True), \
            StructField("destination_account", StringType(), True), \
            StructField("transaction_timestamp", StringType(), True), \
            StructField("transaction_amount", IntegerType(), True), \
            StructField("transaction_type", StringType(), True), \
            StructField("transaction_status", StringType(), True), \
            StructField("transaction_description", StringType(), True), \
            StructField("transaction_fee", StringType(), True), \
            StructField("transaction_currency", StringType(), True), \
            StructField("hour", StringType(), True), \
            StructField("day", StringType(), True), \
            StructField("month", StringType(), True), \
            StructField("year", StringType(), True) \
 \
            ])

        data_info_enrich = [
            ("account123", "omer@example.com", "123456789", 1000, 5000, "False"),
            ("account456", "tal@example.com", "987654321", 1500, 3000, "True")
            # Add more rows here
        ]

        # Create a DataFrame from the sample data
        schema_info = StructType([ \
            StructField("source_account_event", StringType(), True),
            StructField("Email", StringType(), True), \
            StructField("Phone Number", StringType(), True), \
            StructField("Weekly Transfer Amount", IntegerType(), True), \
            StructField("Transfer Limit", IntegerType(), True), \
            StructField("Blocked Status", StringType(), True), \
 \
            ])

        df = spark.createDataFrame(data, schema)
        df_info = spark.createDataFrame(data_info_enrich, schema_info)

        df.createOrReplaceTempView("account_transfer_account_event_partitioned")
        df_info.createOrReplaceTempView("account_transfer_account_info")
        df.show()
        df_info.show()
    def test_phone_used_by_account(self):

        my_datetime = datetime(2023, 8, 12, 15, 30, 0)  # Year, Month, Day, Hour, Minute, Second

        spark = SparkSession.builder \
            .appName("DataEnricher1") \
            .master("local[*]").getOrCreate()

        self.clean_dirty_test(spark)
        self.preper_data(spark)

        account_transfer_account = AccountTransactionAccount(spark)

        account_transfer_account.join_kafka_with_table(my_datetime, "account_transfer_account_event")
        spark.table("account_transfer_account_event_proccesed").show()
        assert spark.table("account_transfer_account_event_proccesed").count() == 2
        spark.stop()


if __name__ == '__main__':
    unittest.main()