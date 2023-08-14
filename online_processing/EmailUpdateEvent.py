from EventProcessor import EventProcessor


class EmailUpdateEventProcessor(EventProcessor):
    def handle(self):
        email_update_event = EmailUpdateEvent(self.online_process, self.json_data)
        email_update_event.activate_all()


class EmailUpdateEvent:
    def __init__(self, online_process, json_data):
        self.online_process = online_process
        self.json_data = json_data

    def activate_all(self):
        self.emailUsedByAccount()

    def emailUsedByAccount(self):
        json_output = self.json_data.selectExpr("data.account", "data.email")
        self.online_process.write_to_kafka(producer="demo_cons", output=json_output)
