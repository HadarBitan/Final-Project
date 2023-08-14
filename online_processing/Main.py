if __name__ == '__main__':
    from online_processing.online_process import OnlineProcess

    # Create an instance of the online_process class
    online_processor = OnlineProcess()

    # Read data from Kafka using Spark Structured Streaming
    kafka_stream = online_processor.read_from_kafka()

    # Process incoming messages
    query = kafka_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda batch_df, batch_id: online_processor.process_event(batch_df)) \
        .start()

    # Wait for the streaming query to finish
    query.awaitTermination()
