from pyspark.sql import SparkSession


def kafka_writer(processed_data):
    # Set up the SparkSession
    spark = SparkSession.builder.appName("KafkaWriter").getOrCreate()

    # Set Kafka broker address
    kafka_brokers = "localhost:9092"

    # Set Kafka topic to write to
    kafka_topic = "processedData"

    # Write the processed data back to Kafka
    processed_data.write.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("topic", kafka_topic) \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .save()

    # Start the streaming context
    spark.streams.awaitAnyTermination()


# Call the Kafka writer function
kafka_writer()
