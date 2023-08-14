from EventProcessor import EventProcessor


class PhoneUpdateEventProcessor(EventProcessor):
    def handle(self):
        phone_update_event = PhoneUpdateEvent(self.json_data)
        phone_update_event.activate_all()


class PhoneUpdateEvent:
    def __init__(self, online_process, json_data):
        self.online_process = online_process
        self.json_data = json_data

    def activate_all(self):
        self.phoneUsedByAccount()

    def phoneUsedByAccount(self):
        json_output = self.json_data.selectExpr("data.account", "data.phone")
        self.online_process.write_to_kafka(producer="demo_cons", output=json_output)
