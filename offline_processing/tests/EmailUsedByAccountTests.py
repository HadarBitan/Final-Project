import unittest
from offline_processing.EmailUsedByAccount import EmailUsedByAccount
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col


class TestEmailUsedByAccount(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize a Spark session for the tests
        cls.spark = SparkSession.builder \
            .appName("unittests") \
            .master("local[*]") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.host", "localhost") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session after all tests are done
        cls.spark.stop()

    def setUp(self):
        self.my_datetime = datetime(2023, 8, 12, 15, 30, 0)

        data = [
            ("2023-08-12 15:30:00", "user@example.com", "account123", "15", "12", "08", "2023"),
            ("2023-08-12 16:45:00", "user2@example.com", "account456", "15", "12", "08", "2023")
        ]

        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("email", StringType(), True),
            StructField("pp_account", StringType(), True),
            StructField("hour", StringType(), True),
            StructField("day", StringType(), True),
            StructField("month", StringType(), True),
            StructField("year", StringType(), True)
        ])

        # Create a DataFrame with the test data
        self.df = self.spark.createDataFrame(data, schema)
        self.df.createOrReplaceTempView("email_changed_event")

    def test_join_kafka_with_table(self):
        # Instantiate the EmailUsedByAccount class
        email_by_pp_account = EmailUsedByAccount(self.spark)

        # Call the function you want to test
        email_by_pp_account.join_kafka_with_table(self.my_datetime, "email_changed_event")

        # Assert the results or perform other tests as needed
        result_df = self.spark.table("email_changed_event_proccesed")

        # Example assertions
        self.assertEqual(result_df.count(), 2)
        self.assertEqual(result_df.columns,
                         ["timestamp", "email", "pp_account", "hour", "day", "month", "year", "email_created_timestmap",
                          "email_last_used", "backup_email", "email_owner_name"])

    def test_email_owner_name_not_null(self):
        # Instantiate the EmailUsedByAccount class
        email_by_pp_account = EmailUsedByAccount(self.spark)

        # Call the function you want to test
        email_by_pp_account.join_kafka_with_table(self.my_datetime, "email_changed_event")

        # Assert that email_owner_name is not null in the result DataFrame
        result_df = self.spark.table("email_changed_event_proccesed")
        self.assertFalse(result_df.filter(col("email_owner_name").isNull()).count() > 0)


if __name__ == '__main__':
    unittest.main()
