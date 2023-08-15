import unittest
from unittest.mock import patch, mock_open, MagicMock
import online_processing.IPUpdateEvent as eventType
from online_processing.online_process import OnlineProcess


class TestIPUpdateEventProcessor(unittest.TestCase):
    @patch("online_processing.IPUpdateEvent.IPUpdateEvent")
    def test_handle(self, mock_ip_update_event):
        mock_json_data = '{"account": "user123", "IP": "123.12.0.25"}'
        event_processor = eventType.IPUpdateEventProcessor(mock_json_data)

        # Set up mock expectations for IPUpdateEvent
        mock_event_instance = mock_ip_update_event.return_value
        event_processor.handle()

        # Assert that IPUpdateEvent.activate_all() was called
        mock_event_instance.activate_all.assert_called_once()


class TestIPUpdateEvent(unittest.TestCase):
    def test_ipUsedByAccount(self):
        mock_json_data = '{"account": "user123", "IP": "123.12.0.25"}'
        event = eventType.IPUpdateEvent(mock_json_data)

        with patch.object(OnlineProcess, "write_to_kafka") as mock_write_to_kafka:
            event.activate_all()

        mock_write_to_kafka.assert_called_once_with(
            producer_topic="demo_cons", output=mock_json_data
        )


if __name__ == "__main__":
    unittest.main()
