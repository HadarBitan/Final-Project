from abc import ABC
from pyspark.shell import spark
from online_processing.online_process import online_procees


class EmailUpdadEvent(online_procees, ABC):

    def extract_data_from_json(self):
        return

    def emailUsedByAccount(self):
        """
        in this function we create an edge that connect between an email to the account that belong to it
        """