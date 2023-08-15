import unittest
from unittest.mock import patch
import online_processing.CreditCardUpdateEvent as eventType
from online_processing.online_process import OnlineProcess


class TestCreditCardUpdateEventProcessor(unittest.TestCase):
    @patch("online_processing.CreditCardUpdateEvent.CreditCardUpdateEvent")
    def test_handle(self, mock_creditcard_update_event):
        mock_json_data = '{"account": "user123", "credit_card": 23628139527}'
        event_processor = eventType.CreditCardUpdateEventProcessor(mock_json_data)

        # Set up mock expectations for CreditCardUpdateEvent
        mock_event_instance = mock_creditcard_update_event.return_value
        event_processor.handle()

        # Assert that CreditCardUpdateEvent.activate_all() was called
        mock_event_instance.activate_all.assert_called_once()


class TestCreditCardUpdateEvent(unittest.TestCase):
    def test_creditCardUsedByAccount(self):
        mock_json_data = '{"account": "user123", "credit_card": 23628139527}'
        event = eventType.CreditCardUpdateEvent(mock_json_data)

        with patch.object(OnlineProcess, "write_to_kafka") as mock_write_to_kafka:
            event.activate_all()

        mock_write_to_kafka.assert_called_once_with(
            producer_topic="demo_cons", output=mock_json_data
        )


if __name__ == "__main__":
    unittest.main()
