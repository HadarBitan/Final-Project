from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
#docker-compose up -d
import os
os.environ['PYSPARK_PYTHON'] = 'python'

def write_to_cassanra(spark, table_name):
    hive_df = spark.sql(f"SELECT * FROM {table_name}")
    hive_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace="test_keyspace") \
        .save()


spark = SparkSession.builder \
    .appName("HiveToCassandra") \
    .config("spark.jars.packages","com.datastax.spark:spark-cassandra-connector_2.12:3.0.0-alpha2")\
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .enableHiveSupport() \
    .getOrCreate()

# data_info_enrich = [
#     ("555-987-6543", "2023-08-01 09:00:00", "555-555-5555", "2023-08-12 15:30:00",
#      "2023-08-10 08:45:00",
#      "2023-08-12 "
#      "14:30:00"),
#     ("555-987-6543", "2023-07-15 14:30:00", "555-999-8888", "2023-08-12 12:00:00",
#      "2023-08-11 09:15:00",
#      "2023-08-12 "
#      "18:45:00")
    # Add more rows here
# here]

# Create a DataFrame from the sample data
#
# schema_info = StructType([ \
#     StructField("phone", StringType(), True),
#     StructField("phone_created_timestamp", StringType(), True), \
#     StructField("backup_phone_number", StringType(), True), \
#     StructField("last_app_login_time", StringType(), True), \
#     StructField("last_security_alert_time", StringType(), True), \
#     StructField("last_purchase_time", StringType(), True) \
#     ])
#
# df_info = spark.createDataFrame(data_info_enrich, schema_info)
# df_info.createTempView("account_transfer_account_event_proccesed")
# df_info.show()


table_names = ["account_transfer_account_event_proccesed", "credit_card_added_event_proccesed","ip_updated_event_proccesed", "phone_updated_event_proccesed","email_changed_event_proccesed"]
for table in table_names:
    write_to_cassanra(spark, table)
