from cassandra.cluster import Cluster
from datetime import datetime, timedelta


class CassandraClient:
    def __init__(self, cluster, session):
        self.cluster = Cluster(cluster)
        self.session = self.cluster.connect(session)

    def close(self):
        self.cluster.shutdown()

    def insert_approval_status(self, transaction_number, approval_status):
        print("hiiB")
        query = "INSERT INTO transaction_approval_status (transaction_number, approval_status) VALUES (%s, %s)"
        self.session.execute(query, (transaction_number, approval_status))

    def process_transactions(self):
        print("hadar")
        query = "SELECT * FROM account_transfer_account_event_processed"
        result = self.session.execute(query)

        for row in result:
            transaction_number = row.number_of_transfer
            self.malicious_check_by_ip(transaction_number)
            self.malicious_check_by_time(transaction_number)
            self.malicious_check_by_amount(transaction_number)
            self.malicious_check_by_doubletransactions(transaction_number)
            self.malicious_check_by_ip_dest(transaction_number)
            self.analyze_transaction(transaction_number)
            self.is_transfer_after_card_expiry(transaction_number)

    # checking if the ip address of the sender has been marked as malicious before
    def get_malicious_ips(self):
        # Fetches the malicious IP addresses from the malicious_accounts table
        query = "SELECT sender_ip FROM malicious_accounts"
        rows = self.session.execute(query)
        malicious_ips = {row.sender_ip for row in rows}
        return malicious_ips

    def malicious_check_by_ip(self, transaction_number):  # -> first_methods
        # Checks if any source IP is in the list of malicious IPs
        malicious_ips = self.get_malicious_ips()
        # Define the query
        query = "SELECT source_account FROM account_transfer_account_event_processed WHERE number_of_transfer = ?"
        # Execute the query
        result = self.session.execute(query, (transaction_number,))
        print(result)
        if result[0].source_account in malicious_ips:
            approval_status = "Not Approved - Malicious IP"
        else:
            approval_status = "Approved"
        self.insert_approval_status(transaction_number, approval_status)

    # checking if the sender has been silent for over a year
    def get_last_transaction_dates(self, transaction_number):
        # Retrieves the last transaction dates for each user
        query = "SELECT Email, MAX(transaction_timestamp) AS last_transaction FROM account_transfer_account_event_proccesed WHERE transaction_number = % s GROUP BY Email"
        rows = self.session.execute(query, (transaction_number,))
        last_transaction_dates = {row.Email: row.last_transaction for row in rows}
        return last_transaction_dates

    def malicious_check_by_time(self, transaction_number):  # -> second_method
        # Checks if it has been at least a year since the last user transaction
        last_transaction_dates = self.get_last_transaction_dates(transaction_number)

        current_date = datetime.now()
        one_year_ago = current_date - timedelta(days=365)

        for email, last_transaction in last_transaction_dates.items():
            if last_transaction < one_year_ago:
                approval_status = "Not Approved - hasn't been active for a year"
            else:
                approval_status = "Approved"
            self.insert_approval_status(transaction_number, approval_status)

    # checking if a transaction is over the average amount of usual transactions
    def get_last_transaction_amounts(self, transaction_number):
        query = "SELECT Email, transaction_amount FROM account_transfer_account_event_proccesed WHERE transaction_number = % s"
        rows = self.session.execute(query, (transaction_number,))
        transaction_amounts = {}

        for row in rows:
            transaction_amounts.setdefault(row.Email, []).append(float(row.transaction_amount))

        return transaction_amounts

    def malicious_check_by_amount(self, transaction_number):  # third method
        transaction_amounts = self.get_last_transaction_amounts(transaction_number)

        last_amount = 0
        average_previous = 0
        for email, amounts in transaction_amounts.items():
            if len(amounts) >= 2:
                last_amount = amounts[-1]
                previous_amounts = amounts[:-1]
                average_previous = sum(previous_amounts) / len(previous_amounts)

        if last_amount >= 1.5 * average_previous:
            approval_status = "Not Approved - suspicious amount of transfer"
        else:
            approval_status = "Approved"
        self.insert_approval_status(transaction_number, approval_status)

    # checking if a 2 transactions has been execute from the same source within 5 minutes from one another
    def get_last_transaction_timestamps(self, source_account):
        query = "SELECT MAX(transaction_timestamp) AS last_transaction FROM account_transfer_account_event_proccesed" \
                "WHERE source_account = %s AND " \
                "transaction_timestamp < (SELECT MAX(transaction_timestamp) FROM account_transfer_account_event_proccesed WHERE source_account = %s)"
        rows = self.session.execute(query, (source_account,))
        last_transaction_timestamps = {row.last_transaction for row in rows}
        return last_transaction_timestamps

    def malicious_check_by_doubletransactions(self, transaction_number):
        query = "SELECT source_account FROM account_transfer_account_event_proccesed WHERE transaction_number = %s"
        rows = self.session.execute(query, (transaction_number,))
        source_account = ""
        for row in rows:
            source_account = row.source_account
        # four method -> verification faceID or password
        last_transaction_timestamps = self.get_last_transaction_timestamps(source_account)

        current_date = datetime.now()
        five_minutes_ago = current_date - timedelta(minutes=5)

        for last_transaction in last_transaction_timestamps:
            if last_transaction >= five_minutes_ago:
                approval_status = "Not Approved - last transfer was last then 5 minuets ago"
            else:
                approval_status = "Approved"
            self.insert_approval_status(transaction_number, approval_status)

    # checking if the ip address of the receiver has been marked as malicious before
    def malicious_check_by_ip_dest(self, transaction_number):
        malicious_ips = self.get_malicious_ips()

        query = "SELECT dst_type FROM account_transfer_account_event_proccesed WHERE transaction_number = %s"
        rows = self.session.execute(query, (transaction_number,))

        for row in rows:
            if row.dst_type in malicious_ips:
                approval_status = "Not Approved - Malicious destination IP"
            else:
                approval_status = "Approved"
            self.insert_approval_status(transaction_number, approval_status)

    # checking if the account transfer the money to an account that a day before transfer the same amount
    def check_previous_transfer(self, dst_account, timestamp, transaction_amount):
        # Calculate the timestamp for the previous day
        previous_day = timestamp - timedelta(days=1)

        # Prepare and execute the query to check for a previous transfer
        query = f"SELECT * FROM account_transfer_account_event_proccesed WHERE src = %s AND" \
                f" transaction_timestamp >= %s AND transaction_timestamp <= %s AND transaction_amount = %s"
        result = self.session.execute(query, (dst_account, dst_account, previous_day, timestamp, transaction_amount))

        return len(result.current_rows) > 0

    def analyze_transactions(self, transaction_number):
        # Query and process data from the tables
        query = "SELECT dst, timestamp, transaction_amount FROM account_transfer_account_event_proccesed WHERE transaction_number = %s"
        result = self.session.execute(query, (transaction_number,))

        for row in result:
            dst_account = row.dst
            timestamp = row.timestamp
            transaction_amount = row.transaction_amount

            has_previous_transfer = self.check_previous_transfer(dst_account, timestamp, transaction_amount)

            if has_previous_transfer:
                approval_status = "Not Approved - possible money laundering"
            else:
                approval_status = "Approved"
            self.insert_approval_status(transaction_number, approval_status)

    # checking if a transfer was made after the expiration date of the credit card
    def is_transfer_after_card_expiry(self, transaction_number):
        query = f"SELECT transaction_timestamp, source_account FROM account_transfer_account_event_proccesed WHERE transaction_number = %s"
        rows = self.session.execute(query, (transaction_number,))

        for row in rows:
            transaction_timestamp = row.timestamp
            source_account = row.source_account

        query = f"SELECT expiration_date FROM credit_card_added_proccesed WHERE account_number = '{source_account}'"
        result = self.session.execute(query)

        for card_row in result:
            expiration_date = card_row.expiration_date
            if expiration_date and transaction_timestamp > expiration_date:
                approval_status = "Not Approved - credit card expired"
            else:
                approval_status = "Approved"
            self.insert_approval_status(transaction_number, approval_status)


if __name__ == '__main__':
    print("hiiiC")
    cluster_number = ["127.0.0.1"]
    keyspace = "final_project"
    cassandra_client = CassandraClient(cluster_number, keyspace)

    cassandra_client.process_transactions()

    cassandra_client.close()

