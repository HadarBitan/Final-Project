import json

from online_processing.EventProcessor import EventProcessor
from online_processing.online_process import OnlineProcess


class PhoneUpdateEventProcessor(EventProcessor):
    def handle(self):
        phone_update_event = PhoneUpdateEvent(self.json_data)
        phone_update_event.activate_all()


class PhoneUpdateEvent:
    def __init__(self, json_data):
        self.data = json.loads(json_data)

    def activate_all(self):
        self.phoneUsedByAccount()

    def phoneUsedByAccount(self):
        # extract the ip of the account and account fields
        account = self.data.get("account")
        phone = self.data.get("phone")
        # creating the json massage to send to kafka
        new_json = {"account": account, "phone": phone}
        # Convert the new JSON object to a string
        json_output = json.dumps(new_json)
        # Write the JSON output to Kafka using the OnlineProcess class
        OnlineProcess().write_to_kafka(producer_topic="demo_cons", output=json_output)
