import json
from online_processing.online_process import OnlineProcess
from pyspark.sql.streaming import StreamingQuery

from TransactionEvent import TransactionEventProcessor
from EmailUpdateEvent import EmailUpdateEventProcessor
from CreditCardAddedEvent import CreditCardUpdateEventProcessor
from IPUpdatedEvent import IPUpdateEventProcessor
from PhoneUpdatedEvent import PhoneUpdateEventProcessor


def process_event(data_json):
    """
    in this function we want to extract from the json we get the event type so we can process the data
    :param data_json: the json we got drom kafka
    :return: a string of the event type
    """
    data = json.loads(data_json)
    event_type = data.get("event_type").lower().replace(" ", "")

    event_processor_classes = {
        "transaction": TransactionEventProcessor,
        "emailupdate": EmailUpdateEventProcessor,
        "creditcardadd": CreditCardUpdateEventProcessor,
        "ipaddressupdate": IPUpdateEventProcessor,
        "phonenumberupdate": PhoneUpdateEventProcessor
    }

    processor_class = event_processor_classes.get(event_type, None)
    if processor_class:
        processor_instance = processor_class(data_json)
        processor_instance.handle()
    else:
        print("Event type not found:", event_type)


def process_batch(batch_df, batch_id):
    """
    Define the processing logic using a foreachBatch transformation
    """
    events = batch_df.collect()
    for row in events:
        event = row["value"]
        process_event(event)


if __name__ == '__main__':
    # Create an instance of the online_process class
    online_processor = OnlineProcess()

    # Read data from Kafka using Spark Structured Streaming
    kafka_stream = online_processor.read_from_kafka()
    # Process each batch of messages

    query = kafka_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .start()

    # Await the termination of the query
    query.awaitTermination()