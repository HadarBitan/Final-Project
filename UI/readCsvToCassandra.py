
import csv

from cassandra.cluster import Cluster


DATA_FILE_PATH = '../UI/data.csv'

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('final_project')

# Open and read the CSV file
with open(DATA_FILE_PATH, 'r') as file:
    is_first_row = True
    for row in csv.reader(file):
        if is_first_row:
            is_first_row = False
        else:
            query = ("INSERT INTO users "
                     "(sender_ip, account_number, is_malicious, money_transfer_amount, online_status,operating_system) "
                     "VALUES "
                     "('{0}', '{1}', {2}, {3}, '{4}', '{5}')").format(row[0], row[1], bool(row[2]), row[3], row[4], row[5])
            print(query)
            session.execute(query)

# Close the connection
cluster.shutdown()
