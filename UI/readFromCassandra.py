import csv
import random
from cassandra.cluster import Cluster

# Establish a connection to the Cassandra cluster
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('final_project')

# Execute a query to fetch all rows from the table
select_query = "SELECT * FROM my_table"
rows = session.execute(select_query)

# Convert the rows to a list
rows_list = list(rows)

# Shuffle the list randomly
random.shuffle(rows_list)

# Specify the output file name
output_file = 'data.csv'

# Open the file in write mode
with open(output_file, 'w', newline='') as csvfile:
    # Create a CSV writer
    writer = csv.writer(csvfile)

    # Write the header row
    writer.writerow(['Sender IP', 'Account Number', 'Is Malicious', 'Money Transfer Amount', 'Online Status', 'Operating System'])

    # Process the shuffled rows
    for row in rows_list:
        # Access individual columns using row.column_name
        sender_ip = row.sender_ip
        account_number = row.account_number
        is_malicious = row.is_malicious
        money_transfer_amount = row.money_transfer_amount
        online_status = row.online_status
        operating_system = row.operating_system

        # Write the row to the CSV file
        writer.writerow([sender_ip, account_number, is_malicious, money_transfer_amount, online_status, operating_system])

# Close the connection
cluster.shutdown()
