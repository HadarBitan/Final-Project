from online_processing.online_process import online_procees


class EmailUpdadEvent:

    def __init__(self, json_data):
        # Extract the fields from the parsed JSON data
        self.account = json_data.select("data.account")
        self.src = json_data.select("data.src")

        self.activate_all()

    def activate_all(self):
        # activate all the creation of edges
        self.ipSrcUsedByAccount()

    def ipSrcUsedByAccount(self):
        """
        in this function we create an edge that connect between an ip address of the sender to the number of transfer
        """
        json_output = self.account.selectExpr("account", "src")
        online_procees.write_to_kafka(producer="demo_cons", output=json_output)
