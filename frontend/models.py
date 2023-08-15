import csv
import json
import os.path
import random
import calendar

from faker import Faker
from datetime import datetime, timedelta
from collections import Counter
from calendar import monthrange

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


def count_blocked_users_last_week():
    # Connect to the Cassandra cluster
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('final_project')

    # Calculate the date range for the last week
    today = datetime.today()
    last_week_start = today - timedelta(days=today.weekday() + 7)
    seven_days_ago = datetime.now() - timedelta(days=7)
    seven_days_ago_str = str(seven_days_ago)

    # Extract only the date part
    date_only = seven_days_ago_str.split(" ")[0]  # Initialize a Counter to store blocked user counts for each day
    blocked_user_counts = Counter()
    # print(date_only)
    # Query to fetch blocked users within the last week using ALLOW FILTERING
    query = "SELECT * FROM malicious_accounts WHERE last_activity >= %s ALLOW FILTERING"
    result = session.execute(query, [date_only])
    # print(result)
    # Count blocked users for each day directly while iterating through the result
    for row in result:
        last_activity = row.last_activity.date()
        blocked_user_counts[last_activity] += 1
    # Close the connection
    cluster.shutdown()

    # Create an array of size 7 with blocked user counts for each day of the last week
    last_week_blocked_counts = []
    current_day = last_week_start
    for _ in range(7):
        last_week_blocked_counts.append(blocked_user_counts[current_day.date()])
        current_day += timedelta(days=1)
    return last_week_blocked_counts


def count_blocked_users_last_month():
    # Connect to the Cassandra cluster
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('final_project')

    # Calculate the date range for the last month
    today = datetime.today()
    last_month_start = today.replace(day=1) - timedelta(days=1)
    last_month_start = last_month_start.replace(day=1)

    _, last_day_of_last_month = monthrange(last_month_start.year, last_month_start.month)
    last_month_end = last_month_start.replace(day=last_day_of_last_month)

    # Initialize a Counter to store blocked user counts for each week
    blocked_user_counts = Counter()

    last_month_start_str = str(last_month_start)
    last_month_end_str = str(last_month_end)
    date_only_start = last_month_start_str.split(" ")[0]
    date_only_end = last_month_end_str.split(" ")[0]

    # Query to fetch blocked users within the last month using ALLOW FILTERING
    query = "SELECT * FROM malicious_accounts WHERE last_activity >= %s AND last_activity <= %s ALLOW FILTERING"
    result = session.execute(query, [date_only_start, date_only_end])
    #print(result)

    # Count blocked users for each week directly while iterating through the result
    for row in result:
        last_activity = row.last_activity.date()
        week_start = last_activity - timedelta(days=last_activity.weekday())
        blocked_user_counts[week_start] += 1
    #print(blocked_user_counts)
    # Close the connection
    cluster.shutdown()
    #print("###", blocked_user_counts.keys())
    # Extract values from the blocked_user_counts dictionary and put them in an array of size 4
    last_month_blocked_counts = []
    for key in blocked_user_counts.keys():
        last_month_blocked_counts.append(blocked_user_counts[key])

    # Ensure the array has a size of 4
    last_month_blocked_counts = last_month_blocked_counts[:4]

    return last_month_blocked_counts


def count_blocked_users_per_month_2023():
    # Connect to the Cassandra cluster
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('final_project')

    # Initialize a Counter to store blocked user counts for each month
    blocked_user_counts = Counter()

    # Query to fetch blocked users for the year 2023 using ALLOW FILTERING
    query = "SELECT * FROM malicious_accounts WHERE last_activity >= '2023-01-01' AND last_activity < '2024-01-01' ALLOW FILTERING"
    result = session.execute(query)

    # Count blocked users for each month directly while iterating through the result
    for row in result:
        last_activity = row.last_activity.date()
        month_start = last_activity.replace(day=1)
        blocked_user_counts[month_start] += 1
    print(blocked_user_counts)
    # Close the connection
    cluster.shutdown()

    # Extract values from the blocked_user_counts dictionary and put them in an array of size 12
    blocked_users_per_month = []
    for key in blocked_user_counts.keys():
        blocked_users_per_month.append(blocked_user_counts[key])

    # Ensure the array has a size of 12
    blocked_users_per_month = blocked_users_per_month[:12]

    # Print the blocked user counts for each month (for debugging)
    print(blocked_users_per_month)

    return blocked_users_per_month
