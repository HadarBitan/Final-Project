from online_processing import EventProcessor
from online_processing.online_process import online_process


class IPUpdateEventProcessor(EventProcessor):
    def handle(self):
        ip_update_event = IPUpdateEvent(self.json_data)
        ip_update_event.activate_all()


class IPUpdateEvent:
    def __init__(self, json_data):
        self.json_data = json_data

    def activate_all(self):
        self.ipUsedByAccount()

    def ipUsedByAccount(self):
        json_output = self.json_data.selectExpr("data.account", "data.ip")
        online_process.write_to_kafka(producer="demo_cons", output=json_output)
