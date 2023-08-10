from online_processing import EventProcessor
from online_processing.online_process import online_process


class PhoneUpdateEventProcessor(EventProcessor):
    def handle(self):
        phone_update_event = PhoneUpdateEvent(self.json_data)
        phone_update_event.activate_all()


class PhoneUpdateEvent:
    def __init__(self, json_data):
        self.json_data = json_data

    def activate_all(self):
        self.phoneUsedByAccount()

    def phoneUsedByAccount(self):
        json_output = self.json_data.selectExpr("data.account", "data.phone")
        online_process.write_to_kafka(producer="demo_cons", output=json_output)
