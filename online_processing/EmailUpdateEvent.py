from online_processing import EventProcessor
from online_processing.online_process import online_process


class EmailUpdateEventProcessor(EventProcessor):
    def handle(self):
        email_update_event = EmailUpdateEvent(self.json_data)
        email_update_event.activate_all()


class EmailUpdateEvent:
    def __init__(self, json_data):
        self.json_data = json_data

    def activate_all(self):
        self.emailUsedByAccount()

    def emailUsedByAccount(self):
        json_output = self.json_data.selectExpr("data.account", "data.email")
        online_process.write_to_kafka(producer="demo_cons", output=json_output)
