from pyspark.sql import SparkSession


def write_to_cassanra(spark, table_name):
    hive_df = spark.sql(f"SELECT * FROM {table_name}")
    hive_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="cassandra_table", keyspace="cassandra_keyspace") \
        .save()


spark = SparkSession.builder \
    .appName("HiveToCassandra") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "username") \
    .config("spark.cassandra.auth.password", "password") \
    .enableHiveSupport() \
    .getOrCreate()


table_names = ["account_transfer_account_event_proccesed", "credit_card_added_event_proccesed","ip_updated_event_proccesed", "phone_updated_event_proccesed","email_changed_event_proccesed"]
for table in table_names:
    write_to_cassanra(spark, table)
