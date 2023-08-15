import json

from online_processing.EventProcessor import EventProcessor
from online_processing.online_process import OnlineProcess


class CreditCardUpdateEventProcessor(EventProcessor):
    def handle(self):
        credit_update_event = CreditCardUpdateEvent(self.json_data)
        credit_update_event.activate_all()


class CreditCardUpdateEvent:
    def __init__(self, json_data):
        self.data = json.loads(json_data)

    def activate_all(self):
        self.creditCardUsedByAccount()

    def creditCardUsedByAccount(self):
        # extract the credit card and account fields
        account = self.data.get("account")
        credit_card = self.data.get("credit_card")
        # creating the json massage to send to kafka
        new_json = {"account": account, "credit_card": credit_card}
        # Convert the new JSON object to a string
        json_output = json.dumps(new_json)
        # Write the JSON output to Kafka using the OnlineProcess class
        OnlineProcess().write_to_kafka(producer_topic="demo_cons", output=json_output)
