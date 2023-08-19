import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import os

os.environ['PYSPARK_PYTHON'] = 'python'
from offline_processing.AccountUsedByCreditCard import AccountUsedByCreditCard
import unittest


class TestCreditCardEdge(unittest.TestCase):

    def clean_dirty_test(self, spark):
        if spark.catalog.tableExists("credit_card_added_proccesed"):
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
            ("2023-08-12 15:30:00", 5000, "2028-08-12 ", "Omer", "account123", "15", "12", "08", "2023"),
            ("2023-08-12 16:45:00", 2000, "2024-08-12", "Tal", "account456", "15", "12", "08", "2023")
            # Add more rows here
        ]

        # Create a DataFrame from the sample data

        schema = StructType([ \
            StructField("timestamp", StringType(), True), \
            StructField("credit_limit", IntegerType(), True), \
            StructField("expiration_date", StringType(), True), \
            StructField("cardholder_name", StringType(), True), \
            StructField("account_id", StringType(), True), \
            StructField("hour", StringType(), True), \
            StructField("day", StringType(), True), \
            StructField("month", StringType(), True), \
            StructField("year", StringType(), True) \
 \
            ])

        data_info_enrich = [
            ("account123", "2023-08-12 15:01", "paypal", "0545381648", "111 555"),
            ("account456", "2023-08-15 10:01", "dicont", "05016431861", "111 222")
            # Add more rows here
        ]

        # Create a DataFrame from the sample data

        schema_info = StructType([ \
            StructField("account_number", StringType(), True),
            StructField("last_transfer_time", StringType(), True), \
            StructField("Bank", StringType(), True), \
            StructField("phone_num", StringType(), True), \
            StructField("credit_card_number", StringType(), True), \
 \
            ])

        spark = SparkSession.builder \
            .appName("DataEnricher2") \
            .master("local[*]").getOrCreate()

        if spark.catalog.tableExists("credit_card_added_event_proccesed"):
            # Drop the existing table
            spark.sql("DROP TABLE IF EXISTS credit_card_added_event_proccesed")

        df = spark.createDataFrame(data, schema)
        df_info = spark.createDataFrame(data_info_enrich, schema_info)
        # df_info.write.mode("overwrite").saveAsTable("email_info")

        df.createTempView("credit_card_added_partitioned")
        df_info.createTempView("credit_card_info")
    def test_credit_card_used_by_account(self):

        my_datetime = datetime(2023, 8, 12, 15, 30, 0)  # Year, Month, Day, Hour, Minute, Second

        spark = SparkSession.builder \
            .appName("DataEnricher1") \
            .master("local[*]").getOrCreate()

        self.clean_dirty_test(spark)
        self.preper_data(spark)

        account_used_by_creditCard = AccountUsedByCreditCard(spark)

        account_used_by_creditCard.join_kafka_with_table(my_datetime, "credit_card_added")
        spark.table("credit_card_added_proccesed").show()
        assert spark.table("credit_card_added_proccesed").count() == 2
        spark.stop()


if __name__ == '__main__':
    unittest.main()
