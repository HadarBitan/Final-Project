from EventProcessor import EventProcessor


class CreditCardUpdateEventProcessor(EventProcessor):
    def handle(self):
        credit_update_event = CreditCardUpdateEvent(self.json_data)
        credit_update_event.activate_all()


class CreditCardUpdateEvent:
    def __init__(self, online_process, json_data):
        self.online_process = online_process
        self.json_data = json_data

    def activate_all(self):
        self.creditCardUsedByAccount()

    def creditCardUsedByAccount(self):
        json_output = self.json_data.selectExpr("data.account", "data.credit_card")
        self.online_process.write_to_kafka(producer="demo_cons", output=json_output)
