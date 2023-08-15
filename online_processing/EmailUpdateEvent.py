import json

from online_processing.EventProcessor import EventProcessor
from online_processing.online_process import OnlineProcess


class EmailUpdateEventProcessor(EventProcessor):
    def __init__(self, json_data):
        super().__init__(json_data)

    def handle(self):
        email_update_event = EmailUpdateEvent(self.json_data)
        email_update_event.activate_all()


class EmailUpdateEvent:
    def __init__(self, json_data):
        self.data = json.loads(json_data)

    def activate_all(self):
        self.emailUsedByAccount()

    def emailUsedByAccount(self):
        # extract the email and account fields
        account = self.data.get("account")
        email = self.data.get("email")
        # creating the json massage to send to kafka
        new_json = {"account": account, "email": email}
        # Convert the new JSON object to a string
        json_output = json.dumps(new_json)
        # Write the JSON output to Kafka using the OnlineProcess class
        OnlineProcess().write_to_kafka(producer_topic="demo_cons", output=json_output)
