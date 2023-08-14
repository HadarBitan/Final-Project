from EventProcessor import EventProcessor


class TransactionEventProcessor(EventProcessor):
    def handle(self):
        transaction_event = TransactionEvent(self.online_process, self.json_data)
        transaction_event.activate_all()


class TransactionEvent:
    def __init__(self, online_process, json_data):
        self.online_process = online_process
        self.json_data = json_data

    def activate_all(self):
        self.ipUsedByAccount()
        self.emailUsedByAccount()
        self.ipSrcUsedByNumberOfTransfer()
        self.ipDstUsedByNumberOfTransfer()
        self.regionUsedByAccount()

    def ipUsedByAccount(self):
        json_output = self.json_data.selectExpr("data.account", "data.src")
        self.online_process.write_to_kafka(producer="demo_cons", output=json_output)

    def emailUsedByAccount(self):
        json_output = self.json_data.selectExpr("data.account", "data.email")
        self.online_process.write_to_kafka(producer="demo_cons", output=json_output)

    def ipSrcUsedByNumberOfTransfer(self):
        json_output = self.json_data.selectExpr("data.number_of_transfer", "data.src")
        self.online_process.write_to_kafka(producer="demo_cons", output=json_output)

    def ipDstUsedByNumberOfTransfer(self):
        json_output = self.json_data.selectExpr("data.number_of_transfer", "data.dst")
        self.online_process.write_to_kafka(producer="demo_cons", output=json_output)

    def regionUsedByAccount(self):
        json_output = self.json_data.selectExpr("data.account", "data.props.region as region")
        self.online_process.write_to_kafka(producer="demo_cons", output=json_output)
