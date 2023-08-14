
import csv
import os.path
import random
import calendar

from faker import Faker

from cassandra.cluster import Cluster, ResultSet


DATA_FILE_PATH = 'storage/data.csv'


def upload_csv_data_to_db():
    count = 0

    # Connect to the Cassandra cluster
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('final_project')

    # Open and read the CSV file
    with open(os.path.abspath(DATA_FILE_PATH), 'r') as file:
        is_first_row = True
        rows = []
        for row in csv.reader(file):
            if is_first_row:
                is_first_row = False
            else:
                rows += [row]
                query = ("INSERT INTO malicious_accounts "
                         "(sender_ip, account_number, online_status,operating_system,last_activity) "
                         "VALUES "
                         "('{0}', '{1}', '{2}', '{3}', {4})").format(row[0], row[1], row[2], row[3], row[4])

                session.execute(query)
                count += 1

    # Close the connection
    cluster.shutdown()

    return count


def create_fake_malicious_accounts(n: int) -> None:
    # Connect to the Cassandra cluster
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('final_project')

    faker = Faker()

    account_numbers = []
    while len(account_numbers) < n:
        account_number = ''.join([str(faker.random.randint(0, 9)) for j in range(9)])
        if account_number not in account_numbers:
            account_numbers.append(account_number)

    for i in range(n):
        account_number = account_numbers[i]
        sender_ip = '.'.join([str(faker.random.randint(0, 255)) for j in range(4)])
        online_status = faker.random.choice(['Online', 'Offline'])
        operating_system = faker.random.choice(['Windows', 'MacOS', 'Linux'])

        year = 2023  # Modify the year if needed
        month = faker.random.randint(1, 8)
        max_days = calendar.monthrange(year, month)[1]
        day = faker.random.randint(1, max_days)
        hour = faker.random.randint(0, 23)
        minute = faker.random.randint(0, 59)
        second = faker.random.randint(0, 59)

        # Create the formatted date string
        last_activity = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"

        query = ("INSERT INTO malicious_accounts "
                 "(sender_ip, account_number, online_status, operating_system, last_activity) "
                 "VALUES "
                 "('{0}', '{1}', '{2}', '{3}', '{4}')").format(
            sender_ip, account_number, online_status, operating_system, last_activity)

        print(query)

        session.execute(query)

    # Close the connection
    cluster.shutdown()


def get_malicious_accounts():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('final_project')

    query = "SELECT sender_ip, account_number, online_status,operating_system,last_activity FROM malicious_accounts;"

    rows = session.execute(query)

    cluster.shutdown()

    return [list(row) for row in rows]
