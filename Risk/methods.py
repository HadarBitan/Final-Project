from cassandra.cluster import Cluster
from datetime import datetime, timedelta


class CassandraClient:
    def __init__(self, contact_points, keyspace):
        self.cluster = Cluster(contact_points)
        self.session = self.cluster.connect(keyspace)

    def close(self):
        self.cluster.shutdown()

    def get_malicious_ips(self):
        # Fetches the malicious IP addresses from the malicious_accounts table
        query = "SELECT sender_ip FROM malicious_accounts"
        rows = self.session.execute(query)
        malicious_ips = {row.sender_ip for row in rows}
        return malicious_ips

    def malicious_check_by_ip(self):  # -> first_methods
        # Checks if any source IP is in the list of malicious IPs
        malicious_ips = self.get_malicious_ips()

        query = "SELECT src FROM ip_used_by_account_proccesed"
        rows = self.session.execute(query)

        for row in rows:
            if row.src in malicious_ips:
                return True

        return False

    def get_last_transaction_dates(self):
        # Retrieves the last transaction dates for each user
        query = "SELECT Email, MAX(transaction_timestamp) AS last_transaction FROM account_transfer_account_event_proccesed GROUP BY Email"
        rows = self.session.execute(query)
        last_transaction_dates = {row.Email: row.last_transaction for row in rows}
        return last_transaction_dates

    def malicious_check_by_time(self):  # -> second_method
        # Checks if it has been at least a year since the last user transaction
        last_transaction_dates = self.get_last_transaction_dates()

        current_date = datetime.now()
        one_year_ago = current_date - timedelta(days=365)

        for email, last_transaction in last_transaction_dates.items():
            if last_transaction < one_year_ago:
                return True

        return False

    def get_last_transaction_amounts(self):
        query = "SELECT Email, transaction_amount FROM account_transfer_account_event_proccesed"
        rows = self.session.execute(query)
        transaction_amounts = {}

        for row in rows:
            transaction_amounts.setdefault(row.Email, []).append(float(row.transaction_amount))

        return transaction_amounts

    def malicious_check_by_amount(self):  # third method
        transaction_amounts = self.get_last_transaction_amounts()

        for email, amounts in transaction_amounts.items():
            if len(amounts) >= 2:
                last_amount = amounts[-1]
                previous_amounts = amounts[:-1]
                average_previous = sum(previous_amounts) / len(previous_amounts)

                if last_amount >= 1.5 * average_previous:
                    return True

        return False

    def get_last_transaction_timestamps(self):
        query = "SELECT Email, MAX(transaction_timestamp) AS last_transaction FROM account_transfer_account_event_proccesed GROUP BY Email"
        rows = self.session.execute(query)
        last_transaction_timestamps = {row.Email: row.last_transaction for row in rows}
        return last_transaction_timestamps

    def malicious_check_by_doubletransactions(self):  # four method -> verification faceID or password
        last_transaction_timestamps = self.get_last_transaction_timestamps()

        current_date = datetime.now()
        five_minutes_ago = current_date - timedelta(minutes=5)

        for email, last_transaction in last_transaction_timestamps.items():
            if last_transaction >= five_minutes_ago:
                return True

        return False

    def malicious_check_by_ip_dest(self):
        malicious_ips = self.get_malicious_ips()

        query = "SELECT dst_type FROM account_transfer_account_event_proccesed"
        rows = self.session.execute(query)

        for row in rows:
            if row.dst_type in malicious_ips:
                return True

        return False


if __name__ == "__main__":
    contact_points = ["127.0.0.1"]
    keyspace = "final_project"

    cassandra_client = CassandraClient(contact_points, keyspace)

    result_ip = cassandra_client.malicious_check_by_ip()
    result_time = cassandra_client.malicious_check_by_time()
    result_amount = cassandra_client.malicious_check_by_amount()
    result_double_transactions = cassandra_client.malicious_check_by_doubletransactions()
    result_ip_dest = cassandra_client.malicious_check_by_ip_dest()

    if result_ip:
        print("User is malicious based on IP.")
    else:
        print("User is not malicious based on IP.")

    if result_time:
        print("User is malicious based on time.")
    else:
        print("User is not malicious based on time.")

    if result_amount:
        print("User is malicious based on transaction amount.")
    else:
        print("User is not malicious based on transaction amount.")

    if result_double_transactions:
        print("User is malicious based on double transactions.")
    else:
        print("User is not malicious based on double transactions.")
    if result_ip_dest:
        print("User is malicious based on destination IP.")
    else:
        print("User is not malicious based on destination IP.")

    cassandra_client.close()
