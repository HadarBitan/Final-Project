if __name__ == '__main__':
    # from features_to_db import props_extractor
    from online_processing import online_process

    # Create an instance of the online_process class
    online_processor = online_process()

    # Read data from Kafka using Spark Structured Streaming
    kafka_stream = online_processor.read_from_kafka()

    # Process incoming messages
    query = kafka_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda batch_df, batch_id: online_processor.process_event_batch(batch_df)) \
        .start()

    # Wait for the streaming query to finish
    query.awaitTermination()
