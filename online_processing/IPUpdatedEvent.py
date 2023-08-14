import json

from EventProcessor import EventProcessor
from online_processing.online_process import OnlineProcess


class IPUpdateEventProcessor(EventProcessor):
    def handle(self):
        ip_update_event = IPUpdateEvent(self.json_data)
        ip_update_event.activate_all()


class IPUpdateEvent:
    def __init__(self, json_data):
        self.data = json.loads(json_data)

    def activate_all(self):
        self.ipUsedByAccount()

    def ipUsedByAccount(self):
        # extract the ip of the account and account fields
        account = self.data.get("account")
        ip = self.data.get("IP")
        # creating the json massage to send to kafka
        new_json = {"account": account, "IP": ip}
        # Convert the new JSON object to a string
        json_output = json.dumps(new_json)
        # Write the JSON output to Kafka using the OnlineProcess class
        OnlineProcess().write_to_kafka(producer_topic="demo_cons", output=json_output)
