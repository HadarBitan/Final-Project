from abc import ABC
from pyspark.shell import spark
from online_processing.online_process import online_procees

class TransactionEvent(online_procees, ABC):

    def extract_data_from_json(self):
        data = ""
        return data
