from online_processing import EventProcessor
from online_processing.online_process import online_process


class CreditCardUpdateEventProcessor(EventProcessor):
    def handle(self):
        credit_update_event = CreditCardUpdateEvent(self.json_data)
        credit_update_event.activate_all()


class CreditCardUpdateEvent:
    def __init__(self, json_data):
        self.json_data = json_data

    def activate_all(self):
        self.creditCardUsedByAccount()

    def creditCardUsedByAccount(self):
        json_output = self.json_data.selectExpr("data.account", "data.credit_card")
        online_process.write_to_kafka(producer="demo_cons", output=json_output)
