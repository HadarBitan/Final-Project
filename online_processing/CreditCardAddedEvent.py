from online_processing.online_process import online_procees


class EmailUpdadEvent:

    def __init__(self, json_data):
        # Extract the fields from the parsed JSON data
        self.account = json_data.select("data.account")
        self.creditCard = json_data.select("data.credit_card")

        self.activate_all()

    def activate_all(self):
        # activate all the creation of edges
        self.creditCardUsedByAccount()

    def creditCardUsedByAccount(self):
        """
        in this function we create an edge that connect between an credit_card to the account that belong to it
        """
        json_output = self.account.selectExpr("account", "credit_card")
        online_procees.write_to_kafka(producer="demo_cons", output=json_output)
