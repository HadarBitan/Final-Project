import unittest
from unittest.mock import patch, mock_open, MagicMock
import online_processing.TransactionEvent as eventType
from online_processing.online_process import OnlineProcess


class TestTransactionEventProcessor(unittest.TestCase):
    @patch("online_processing.TransactionEvent.TransactionEvent")
    def test_handle(self, mock_transaction_event):
        mock_json_data = '{"event_type":"Transaction","number_of_transfer": 265482,' \
                         '"account": "A5fV2","ip_sender": "123.12.0.25","ip_receiver": "125.63.5.23",' \
                         '"amount_of_transfer": 500,"email": "aaaa@gmail.com",' \
                         '"props": {"os": "window","time": 324234,"browser": "chrome","region": "Canada"}}'
        event_processor = eventType.TransactionEventProcessor(mock_json_data)

        # Set up mock expectations for TransactionEvent
        mock_event_instance = mock_transaction_event.return_value
        event_processor.handle()

        # Assert that TransactionEvent.activate_all() was called
        mock_event_instance.activate_all.assert_called_once()


class TestTransactionEvent(unittest.TestCase):
    def setUp(self):
        mock_json_data = '{"event_type":"Transaction","number_of_transfer": 265482,"account": "A5fV2",' \
                         '"ip_sender": "123.12.0.25","ip_receiver": "125.63.5.23",' \
                         '"amount_of_transfer": 500,"email": "aaaa@gmail.com",' \
                         '"props": {"os": "window","time": 324234,"browser": "chrome","region": "Canada"}}'
        self.event = eventType.TransactionEvent(mock_json_data)

    def test_ipUsedByAccount(self):
        with patch.object(OnlineProcess, "write_to_kafka") as mock_write_to_kafka:
            self.event.ipUsedByAccount()

        mock_write_to_kafka.assert_called_once_with(
            producer_topic="demo_cons", output='{"account": "A5fV2", "ip_sender": "123.12.0.25"}'
        )

    def test_emailUsedByAccount(self):
        with patch.object(OnlineProcess, "write_to_kafka") as mock_write_to_kafka:
            self.event.emailUsedByAccount()

        mock_write_to_kafka.assert_called_once_with(
            producer_topic="demo_cons", output='{"account": "A5fV2", "email": "aaaa@gmail.com"}'
        )

    def test_ipSrcUsedByNumberOfTransfer(self):
        with patch.object(OnlineProcess, "write_to_kafka") as mock_write_to_kafka:
            self.event.ipSrcUsedByNumberOfTransfer()

        mock_write_to_kafka.assert_called_once_with(
            producer_topic="demo_cons", output='{"number_of_transfer": 265482, "ip_sender": "123.12.0.25"}'
        )

    def test_ipDstUsedByNumberOfTransfer(self):
        with patch.object(OnlineProcess, "write_to_kafka") as mock_write_to_kafka:
            self.event.ipDstUsedByNumberOfTransfer()

        mock_write_to_kafka.assert_called_once_with(
            producer_topic="demo_cons", output='{"number_of_transfer": 265482, "ip_receiver": "125.63.5.23"}'
        )

    def test_regionUsedByAccount(self):
        with patch.object(OnlineProcess, "write_to_kafka") as mock_write_to_kafka:
            self.event.regionUsedByAccount()

        mock_write_to_kafka.assert_called_once_with(
            producer_topic="demo_cons", output='{"account": "A5fV2", "region": "Canada"}'
        )

    def test_activate_all(self):
        with patch.object(self.event, "ipUsedByAccount") as mock_ipUsedByAccount, \
             patch.object(self.event, "emailUsedByAccount") as mock_emailUsedByAccount, \
             patch.object(self.event, "ipSrcUsedByNumberOfTransfer") as mock_ipSrcUsedByNumberOfTransfer, \
             patch.object(self.event, "ipDstUsedByNumberOfTransfer") as mock_ipDstUsedByNumberOfTransfer, \
             patch.object(self.event, "regionUsedByAccount") as mock_regionUsedByAccount:

            self.event.activate_all()

        mock_ipUsedByAccount.assert_called_once()
        mock_emailUsedByAccount.assert_called_once()
        mock_ipSrcUsedByNumberOfTransfer.assert_called_once()
        mock_ipDstUsedByNumberOfTransfer.assert_called_once()
        mock_regionUsedByAccount.assert_called_once()


if __name__ == '__main__':
    unittest.main()
