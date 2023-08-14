from EventProcessor import EventProcessor


class IPUpdateEventProcessor(EventProcessor):
    def handle(self):
        ip_update_event = IPUpdateEvent(self.json_data)
        ip_update_event.activate_all()


class IPUpdateEvent:
    def __init__(self, online_process, json_data):
        self.online_process = online_process
        self.json_data = json_data

    def activate_all(self):
        self.ipUsedByAccount()

    def ipUsedByAccount(self):
        json_output = self.json_data.selectExpr("data.account", "data.ip")
        self.online_process.write_to_kafka(producer="demo_cons", output=json_output)
