import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit
from offline_processing.EmailUsedByAccount import EmailUsedByAccount
import random
import string
import os
os.environ['PYSPARK_PYTHON'] = 'python'


class TestEmailUsedByAccount(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestEmailUsedByAccount") \
            .master("local[*]").getOrCreate()

        cls.schema_email = StructType([
            StructField("email", StringType(), True),
            StructField("email_created_timestmap", StringType(), True),
            StructField("email_last_used", StringType(), True),
            StructField("backup_email", StringType(), True),
            StructField("email_owner_name", StringType(), True),
        ])

        cls.schema_partitioned = StructType([
            StructField("timestamp", StringType(), True),
            StructField("pp_account", StringType(), True),
            StructField("email", StringType(), True),
            StructField("org_email", StringType(), True),
        ])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def random_data(self, num_rows=10):
        data = []
        for _ in range(num_rows):
            email = f"{''.join(random.choices(string.ascii_lowercase, k=5))}@example.com"
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            backup_email = f"backup_{email}"
            owner_name = ''.join(random.choices(string.ascii_uppercase, k=5))
            data.append((email, timestamp, timestamp, backup_email, owner_name))
        return self.spark.createDataFrame(data, self.schema_email)

    def test_join_and_enrich(self):
        enricher = EmailUsedByAccount(self.spark)

        # Generate random data
        num_rows = 5
        df_email = self.random_data(num_rows)

        # Create partitioned DataFrame
        data_partitioned = []
        for _ in range(num_rows):
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            pp_account = ''.join(random.choices(string.digits, k=8))
            email = df_email.select("email").collect()[0]["email"]
            org_email = f"org_{email}"
            data_partitioned.append((timestamp, pp_account, email, org_email))
        df_partitioned = self.spark.createDataFrame(data_partitioned, self.schema_partitioned)

        # Perform join and enrichment
        enriched_df = enricher.join_and_enrich(df_partitioned, df_email)

        expected_columns = [
            "timestamp", "pp_account", "email", "org_email",
            "email_created_timestmap", "email_last_used", "backup_email", "email_owner_name"
        ]

        self.assertTrue(all(col in enriched_df.columns for col in expected_columns))
        self.assertEqual(enriched_df.count(), num_rows)  # Ensure correct number of rows
        # Add more assertions here to validate the enriched data as needed


if __name__ == "__main__":
    unittest.main()
