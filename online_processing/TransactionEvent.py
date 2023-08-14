import json

from EventProcessor import EventProcessor
from online_processing.online_process import OnlineProcess


class TransactionEventProcessor(EventProcessor):
    def handle(self):
        transaction_event = TransactionEvent(self.json_data)
        transaction_event.activate_all()


class TransactionEvent:
    def __init__(self, json_data):
        self.data = json.loads(json_data)

    def activate_all(self):
        self.ipUsedByAccount()
        self.emailUsedByAccount()
        self.ipSrcUsedByNumberOfTransfer()
        self.ipDstUsedByNumberOfTransfer()
        self.regionUsedByAccount()

    def ipUsedByAccount(self):
        # extract the ip of the account and account fields
        account = self.data.get("account")
        src = self.data.get("src")
        # creating the json massage to send to kafka
        new_json = {"account": account, "src": src}
        # Convert the new JSON object to a string
        json_output = json.dumps(new_json)
        # Write the JSON output to Kafka using the OnlineProcess class
        OnlineProcess().write_to_kafka(producer_topic="demo_cons", output=json_output)

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

    def ipSrcUsedByNumberOfTransfer(self):
        # extract the src and number_of_transfer fields
        number_of_transfer = self.data.get("number_of_transfer")
        src = self.data.get("src")
        # creating the json massage to send to kafka
        new_json = {"number_of_transfer": number_of_transfer, "src": src}
        # Convert the new JSON object to a string
        json_output = json.dumps(new_json)
        # Write the JSON output to Kafka using the OnlineProcess class
        OnlineProcess().write_to_kafka(producer_topic="demo_cons", output=json_output)

    def ipDstUsedByNumberOfTransfer(self):
        # extract the dst and number_of_transfer fields
        number_of_transfer = self.data.get("number_of_transfer")
        dst = self.data.get("dst")
        # creating the json massage to send to kafka
        new_json = {"number_of_transfer": number_of_transfer, "dst": dst}
        # Convert the new JSON object to a string
        json_output = json.dumps(new_json)
        # Write the JSON output to Kafka using the OnlineProcess class
        OnlineProcess().write_to_kafka(producer_topic="demo_cons", output=json_output)

    def regionUsedByAccount(self):
        # extract the region and account fields
        account = self.data.get("account")
        region = self.data.get("region")
        # creating the json massage to send to kafka
        new_json = {"account": account, "region": region}
        # Convert the new JSON object to a string
        json_output = json.dumps(new_json)
        # Write the JSON output to Kafka using the OnlineProcess class
        OnlineProcess().write_to_kafka(producer_topic="demo_cons", output=json_output)
