import unittest
from mock import Mock

from methods import CassandraClient  # Import your actual script here


class TestCassandraClient(unittest.TestCase):
    def setUp(self):
        self.contact_points = ["127.0.0.1"]
        self.keyspace = "final_project"
        self.cassandra_client = CassandraClient(self.contact_points, self.keyspace)

    def tearDown(self):
        self.cassandra_client.close()

    def test_malicious_check_by_ip_approve(self):
        mock_session = Mock()
        mock_session.execute.return_value = [{'src': 'safe_ip', 'transaction_number': '123'}]
        self.cassandra_client.session = mock_session

        self.cassandra_client.malicious_check_by_ip('123')

        mock_session.execute.assert_called_with(
            "INSERT INTO transaction_approval_status (transaction_number, approval_status) VALUES (%s, %s)",
            ('123', 'Approved')
        )

    def test_malicious_check_by_ip_not_approve(self):
        mock_session = Mock()
        mock_session.execute.return_value = [{'src': 'malicious_ip', 'transaction_number': '123'}]
        self.cassandra_client.session = mock_session

        self.cassandra_client.malicious_check_by_ip('123')

        mock_session.execute.assert_called_with(
            "INSERT INTO transaction_approval_status (transaction_number, approval_status) VALUES (%s, %s)",
            ('123', 'Not Approved - Malicious IP')
        )

    def test_malicious_check_by_time_approve(self):
        mock_session = Mock()
        mock_session.execute.return_value = [
            {'Email': 'user@example.com', 'last_transaction': '2023-01-01', 'transaction_number': '123'}]
        self.cassandra_client.session = mock_session

        self.cassandra_client.malicious_check_by_time('123')

        mock_session.execute.assert_called_with(
            "INSERT INTO transaction_approval_status (transaction_number, approval_status) VALUES (%s, %s)",
            ('123', 'Approved')
        )

    def test_malicious_check_by_time_not_approve(self):
        mock_session = Mock()
        mock_session.execute.return_value = [{'Email': 'user@example.com', 'last_transaction': '2022-01-01', 'transaction_number': '123'}]
        self.cassandra_client.session = mock_session

        self.cassandra_client.malicious_check_by_time('123')

        mock_session.execute.assert_called_with(
            "INSERT INTO transaction_approval_status (transaction_number, approval_status) VALUES (%s, %s)",
            ('123', 'Not Approved - Inactive Account')
        )

    def test_malicious_check_by_amount_approve(self):
        mock_session = Mock()
        mock_session.execute.return_value = [
            {'Email': 'user@example.com', 'transaction_amount': 100, 'transaction_number': '123'}]
        self.cassandra_client.session = mock_session

        self.cassandra_client.malicious_check_by_amount('123')

        mock_session.execute.assert_called_with(
            "INSERT INTO transaction_approval_status (transaction_number, approval_status) VALUES (%s, %s)",
            ('123', 'Approved')
        )

    def test_malicious_check_by_amount_not_approve(self):
        mock_session = Mock()
        mock_session.execute.return_value = [{'Email': 'user@example.com', 'transaction_amount': 200, 'transaction_number': '123'}]
        self.cassandra_client.session = mock_session

        self.cassandra_client.malicious_check_by_amount('123')

        mock_session.execute.assert_called_with(
            "INSERT INTO transaction_approval_status (transaction_number, approval_status) VALUES (%s, %s)",
            ('123', 'Not Approved - Malicious IP')
        )

    def test_malicious_check_by_doubletransactions_approve(self):
        mock_session = Mock()
        mock_session.execute.return_value = [{'source_account': 'account123'}]
        self.cassandra_client.session = mock_session

        self.cassandra_client.malicious_check_by_doubletransactions('123')

        mock_session.execute.assert_called_with(
            "INSERT INTO transaction_approval_status (transaction_number, approval_status) VALUES (%s, %s)",
            ('123', 'Approved')
        )

    def test_malicious_check_by_doubletransactions_not_approve(self):
        mock_session = Mock()
        mock_session.execute.return_value = [{'source_account': 'account123'}]
        self.cassandra_client.session = mock_session

        self.cassandra_client.malicious_check_by_doubletransactions('123')

        mock_session.execute.assert_called_with(
            "INSERT INTO transaction_approval_status (transaction_number, approval_status) VALUES (%s, %s)",
            ('123', 'Not Approved - Malicious IP')
        )

    def test_malicious_check_by_ip_dest_approve(self):
        mock_session = Mock()
        mock_session.execute.return_value = [{'dst_type': 'safe_dst', 'transaction_number': '123'}]
        self.cassandra_client.session = mock_session

        self.cassandra_client.malicious_check_by_ip_dest('123')

        mock_session.execute.assert_called_with(
            "INSERT INTO transaction_approval_status (transaction_number, approval_status) VALUES (%s, %s)",
            ('123', 'Approved')
        )

    def test_malicious_check_by_ip_dest_not_approve(self):
        mock_session = Mock()
        mock_session.execute.return_value = [{'dst_type': 'malicious_dst', 'transaction_number': '123'}]
        self.cassandra_client.session = mock_session

        self.cassandra_client.malicious_check_by_ip_dest('123')

        mock_session.execute.assert_called_with(
            "INSERT INTO transaction_approval_status (transaction_number, approval_status) VALUES (%s, %s)",
            ('123', 'Not Approved - Malicious IP')
        )

    def test_analyze_transaction_approve(self):
        mock_session = Mock()
        mock_session.execute.return_value = [
            {'dst': 'account123', 'timestamp': '2023-08-10', 'transaction_amount': 50, 'transaction_number': '123'}]
        self.cassandra_client.session = mock_session

        self.cassandra_client.analyze_transaction('123')

        mock_session.execute.assert_called_with(
            "INSERT INTO transaction_approval_status (transaction_number, approval_status) VALUES (%s, %s)",
            ('123', 'Approved')
        )

    def test_analyze_transaction_not_approve(self):
        mock_session = Mock()
        mock_session.execute.return_value = [{'dst': 'account123', 'timestamp': '2023-08-10', 'transaction_amount': 150, 'transaction_number': '123'}]
        self.cassandra_client.session = mock_session

        self.cassandra_client.analyze_transaction('123')

        mock_session.execute.assert_called_with(
            "INSERT INTO transaction_approval_status (transaction_number, approval_status) VALUES (%s, %s)",
            ('123', 'Not Approved - Malicious IP')
        )

    def test_is_transfer_after_card_expiry_approve(self):
        mock_session = Mock()
        mock_session.execute.side_effect = [
            [{'timestamp': '2023-08-15', 'source_account': 'account123'}],
            [{'expiration_date': '2023-08-19'}]
        ]
        self.cassandra_client.session = mock_session

        self.cassandra_client.is_transfer_after_card_expiry('123')

        mock_session.execute.assert_called_with(
            "INSERT INTO transaction_approval_status (transaction_number, approval_status) VALUES (%s, %s)",
            ('123', 'Approved')
        )

    def test_is_transfer_after_card_expiry_not_approve(self):
        mock_session = Mock()
        mock_session.execute.side_effect = [
            [{'timestamp': '2023-08-15', 'source_account': 'account123'}],
            [{'expiration_date': '2023-08-12'}]
        ]
        self.cassandra_client.session = mock_session

        self.cassandra_client.is_transfer_after_card_expiry('123')

        mock_session.execute.assert_called_with(
            "INSERT INTO transaction_approval_status (transaction_number, approval_status) VALUES (%s, %s)",
            ('123', 'Not Approved - Malicious IP')
        )


if __name__ == '__main__':
    unittest.main()
