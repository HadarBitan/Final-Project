from abc import ABC
from pyspark.shell import spark
from online_processing.online_process import online_procees

class TransactionEvent(online_procees, ABC):

    def extract_data_from_json(self):
        # Extract the fields from the parsed JSON data
        # self.number_of_transfer = parsed_df.select("data.number_of_transfer")
        # account = parsed_df.select("data.account")
        # src = parsed_df.select("data.src")
        # dst = parsed_df.select("data.dst")
        # value = parsed_df.select("data.value")
        # email = parsed_df.select("data.email")
        # props = parsed_df.select("data.props").alias("props")
        # data = ""
        # return data

    def ipUsedByAccount(self):
        """
        in this function we create an edge that connect between an ip address to the account that belong to it
        """
        # json_output = number_of_transfer.selectExpr("account", "src")
