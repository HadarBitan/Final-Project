import unittest
from unittest.mock import Mock, patch, call
from datetime import datetime, timedelta
from methods import CassandraClient  # Replace with the actual name of your script


class TestCassandraClient(unittest.TestCase):
    def setUp(self):
        self.mock_session = Mock()
        self.client = CassandraClient(contact_points=["127.0.0.1"], keyspace="final_project")
        self.client.session = self.mock_session

    def test_malicious_check_by_ip_approved(self):
        transaction_number = 1
        self.mock_session.execute.return_value = [
            Mock(src="valid_ip", transaction_number=transaction_number)
        ]

        self.client.get_malicious_ips.return_value = {"malicious_ip"}

        self.client.malicious_check_by_ip(transaction_number)

        self.mock_session.execute.assert_called_once_with(
            "SELECT src FROM ip_used_by_account_proccesed WHERE transaction_number = %s", (transaction_number,)
        )

        self.client.insert_approval_status.assert_called_once_with(transaction_number, "Approved")

    def test_malicious_check_by_ip_not_approved(self):
        transaction_number = 2
        self.mock_session.execute.return_value = [
            Mock(src="malicious_ip", transaction_number=transaction_number)
        ]

        self.client.get_malicious_ips.return_value = {"malicious_ip"}

        self.client.malicious_check_by_ip(transaction_number)

        self.mock_session.execute.assert_called_once_with(
            "SELECT src FROM ip_used_by_account_proccesed WHERE transaction_number = %s", (transaction_number,)
        )

        self.client.insert_approval_status.assert_called_once_with(
            transaction_number, "Not Approved - Malicious IP"
        )

    def test_malicious_check_by_time_approved(self):
        transaction_number = 1
        self.mock_session.execute.return_value = [
            Mock(Email="valid_email", last_transaction=datetime.now() - timedelta(days=364),
                 transaction_number=transaction_number)
        ]

        self.client.malicious_check_by_time(transaction_number)

        self.mock_session.execute.assert_called_once_with(
            "SELECT Email, MAX(transaction_timestamp) AS last_transaction FROM account_transfer_account_event_proccesed GROUP BY Email"
        )

        self.client.insert_approval_status.assert_called_once_with(transaction_number, "Approved")

    def test_malicious_check_by_time_not_approved(self):
        transaction_number = 2
        self.mock_session.execute.return_value = [
            Mock(Email="valid_email", last_transaction=datetime.now() - timedelta(days=366),
                 transaction_number=transaction_number)
        ]

        self.client.malicious_check_by_time(transaction_number)

        self.mock_session.execute.assert_called_once_with(
            "SELECT Email, MAX(transaction_timestamp) AS last_transaction FROM account_transfer_account_event_proccesed GROUP BY Email"
        )

        self.client.insert_approval_status.assert_called_once_with(
            transaction_number, "Not Approved - Malicious IP"
        )

    def test_malicious_check_by_amount_approved(self):
        transaction_number = 1
        self.mock_session.execute.return_value = [
            Mock(Email="valid_email", transaction_amount=100, transaction_number=transaction_number),
            Mock(Email="valid_email", transaction_amount=150, transaction_number=transaction_number),
        ]

        self.client.get_last_transaction_amounts.return_value = {"valid_email": [100, 150]}

        self.client.malicious_check_by_amount(transaction_number)

        self.mock_session.execute.assert_called_once_with(
            "SELECT Email, transaction_amount FROM account_transfer_account_event_proccesed"
        )

        self.client.insert_approval_status.assert_called_once_with(transaction_number, "Approved")

    def test_malicious_check_by_amount_not_approved(self):
        transaction_number = 2
        self.mock_session.execute.return_value = [
            Mock(Email="valid_email", transaction_amount=100, transaction_number=transaction_number),
            Mock(Email="valid_email", transaction_amount=200, transaction_number=transaction_number),
        ]

        self.client.get_last_transaction_amounts.return_value = {"valid_email": [100, 200]}

        self.client.malicious_check_by_amount(transaction_number)

        self.mock_session.execute.assert_called_once_with(
            "SELECT Email, transaction_amount FROM account_transfer_account_event_proccesed"
        )

        self.client.insert_approval_status.assert_called_once_with(
            transaction_number, "Not Approved - Malicious IP"
        )

    def test_malicious_check_by_doubletransactions_approved(self):
        transaction_number = 1
        self.mock_session.execute.return_value = [
            Mock(Email="valid_email", last_transaction=datetime.now() - timedelta(minutes=4),
                 transaction_number=transaction_number)
        ]

        self.client.malicious_check_by_doubletransactions(transaction_number)

        self.mock_session.execute.assert_called_once_with(
            "SELECT Email, MAX(transaction_timestamp) AS last_transaction FROM account_transfer_account_event_proccesed GROUP BY Email"
        )

        self.client.insert_approval_status.assert_called_once_with(transaction_number, "Approved")

    def test_malicious_check_by_doubletransactions_not_approved(self):
        transaction_number = 2
        self.mock_session.execute.return_value = [
            Mock(Email="valid_email", last_transaction=datetime.now() - timedelta(minutes=6),
                 transaction_number=transaction_number)
        ]

        self.client.malicious_check_by_doubletransactions(transaction_number)

        self.mock_session.execute.assert_called_once_with(
            "SELECT Email, MAX(transaction_timestamp) AS last_transaction FROM account_transfer_account_event_proccesed GROUP BY Email"
        )

        self.client.insert_approval_status.assert_called_once_with(
            transaction_number, "Not Approved - Malicious IP"
        )

    def test_malicious_check_by_ip_dest_approved(self):
        transaction_number = 1
        self.mock_session.execute.return_value = [
            Mock(dst_type="valid_dst_type", transaction_number=transaction_number)
        ]

        self.client.get_malicious_ips.return_value = {"malicious_ip"}

        self.client.malicious_check_by_ip_dest(transaction_number)

        self.mock_session.execute.assert_called_once_with(
            "SELECT dst_type FROM account_transfer_account_event_proccesed WHERE transaction_number = %s",
            (transaction_number,)
        )

        self.client.insert_approval_status.assert_called_once_with(transaction_number, "Approved")

    def test_malicious_check_by_ip_dest_not_approved(self):
        transaction_number = 2
        self.mock_session.execute.return_value = [
            Mock(dst_type="malicious_ip", transaction_number=transaction_number)
        ]

        self.client.get_malicious_ips.return_value = {"malicious_ip"}

        self.client.malicious_check_by_ip_dest(transaction_number)

        self.mock_session.execute.assert_called_once_with(
            "SELECT dst_type FROM account_transfer_account_event_proccesed WHERE transaction_number = %s",
            (transaction_number,)
        )

        self.client.insert_approval_status.assert_called_once_with(
            transaction_number, "Not Approved - Malicious IP"
        )

    def test_analyze_transaction_approved(self):
        transaction_number = 1
        self.mock_session.execute.return_value = [
            Mock(dst="valid_dst", timestamp=datetime.now() - timedelta(days=1), transaction_amount=100,
                 transaction_number=transaction_number)
        ]

        self.client.check_previous_transfer.return_value = False

        self.client.analyze_transaction(transaction_number)

        self.mock_session.execute.assert_called_once_with(
            "SELECT dst, timestamp, transaction_amount FROM account_transfer_account_event_proccesed WHERE transaction_number = %s",
            (transaction_number,)
        )

        self.client.check_previous_transfer.assert_called_once_with("valid_dst", Mock(), 100)

        self.client.insert_approval_status.assert_called_once_with(transaction_number, "Approved")

    def test_analyze_transaction_not_approved(self):
        transaction_number = 2
        self.mock_session.execute.return_value = [
            Mock(dst="valid_dst", timestamp=datetime.now() - timedelta(days=1), transaction_amount=100,
                 transaction_number=transaction_number)
        ]

        self.client.check_previous_transfer.return_value = True

        self.client.analyze_transaction(transaction_number)

        self.mock_session.execute.assert_called_once_with(
            "SELECT dst, timestamp, transaction_amount FROM account_transfer_account_event_proccesed WHERE transaction_number = %s",
            (transaction_number,)
        )

        self.client.check_previous_transfer.assert_called_once_with("valid_dst", Mock(), 100)

        self.client.insert_approval_status.assert_called_once_with(
            transaction_number, "Not Approved - Malicious IP"
        )

    def test_is_transfer_after_card_expiry_approved(self):
        transaction_number = 1
        self.mock_session.execute.return_value = [
            Mock(timestamp=datetime.now(), source_account="valid_account", transaction_number=transaction_number)
        ]

        self.mock_session.execute.side_effect = [
            [Mock(expiration_date=datetime.now() + timedelta(days=1))]
        ]

        self.client.is_transfer_after_card_expiry(transaction_number)

        self.mock_session.execute.assert_has_calls([
            Mock(call(
                "SELECT transaction_timestamp, source_account FROM account_transfer_account_event_proccesed WHERE transaction_number = %s",
                (transaction_number,)
            )),
            Mock(call(
                "SELECT expiration_date FROM credit_card_added_proccesed WHERE account_number = 'valid_account'"
            ))
        ])

        self.client.insert_approval_status.assert_called_once_with(transaction_number, "Approved")

    def test_is_transfer_after_card_expiry_not_approved(self):
        transaction_number = 2
        self.mock_session.execute.return_value = [
            Mock(timestamp=datetime.now(), source_account="valid_account", transaction_number=transaction_number)
        ]

        self.mock_session.execute.side_effect = [
            [Mock(expiration_date=datetime.now() - timedelta(days=1))]
        ]

        self.client.is_transfer_after_card_expiry(transaction_number)

        self.mock_session.execute.assert_has_calls([
            Mock(call(
                "SELECT transaction_timestamp, source_account FROM account_transfer_account_event_proccesed WHERE transaction_number = %s",
                (transaction_number,)
            )),
            Mock(call(
                "SELECT expiration_date FROM credit_card_added_proccesed WHERE account_number = 'valid_account'"
            ))
        ])

        self.client.insert_approval_status.assert_called_once_with(
            transaction_number, "Not Approved - Malicious IP"
        )

    def tearDown(self):
        self.client.close()


if __name__ == "__main__":
    unittest.main()
