from pyspark.sql.functions import col

from online_processing.online_process import online_procees


class TransactionEvent:

    def __init__(self, json_data):
        # Extract the fields from the parsed JSON data
        self.number_of_transfer = json_data.select("data.number_of_transfer")
        self.account = json_data.select("data.account")
        self.src = json_data.select("data.src")
        self.dst = json_data.select("data.dst")
        self.value = json_data.select("data.value")
        self.email = json_data.select("data.email")
        self.props = json_data.select("data.props").alias("props")

        self.activate_all()

    def activate_all(self):
        # activate all the creation of edges
        self.ipUsedByAccount()
        self.emailUsedByAccount()
        self.ipSrcUsedByNumberOfTransfer()
        self.ipDstUsedByNumberOfTransfer()
        self.regionUsedByAccount()

    def ipUsedByAccount(self):
        """
        in this function we create an edge that connect between an ip address to the account that belong to it
        """
        json_output = self.number_of_transfer.selectExpr("account", "src")
        online_procees.write_to_kafka(producer="demo_cons", output=json_output)

    def emailUsedByAccount(self):
        """
        in this function we create an edge that connect between an email to the account that belong to it
        """
        json_output = self.number_of_transfer.selectExpr("account", "email")
        online_procees.write_to_kafka(producer="demo_cons", output=json_output)

    def ipSrcUsedByNumberOfTransfer(self):
        """
        in this function we create an edge that connect between an ip address of the sender to the number of transfer
        """
        json_output = self.number_of_transfer.selectExpr("number_of_transfer", "src")
        online_procees.write_to_kafka(producer="demo_cons", output=json_output)

    def ipDstUsedByNumberOfTransfer(self):
        """
        in this function we create an edge that connect between an ip address of the reciver to the number of transfer
        """
        json_output = self.number_of_transfer.selectExpr("number_of_transfer", "dst")
        online_procees.write_to_kafka(producer="demo_cons", output=json_output)

    def regionUsedByAccount(self):
        """
        in this function we create an edge that connect between an region to the account that belong to it
        """
        json_output = self.account.join(self.props, col("account") == col("props.account"))\
            .selectExpr("account", "props.region as region")
        online_procees.write_to_kafka(producer="demo_cons", output=json_output)

