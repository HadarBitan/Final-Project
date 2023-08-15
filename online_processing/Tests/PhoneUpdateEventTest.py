import unittest
from unittest.mock import patch, mock_open, MagicMock
import online_processing.PhoneUpdateEvent as eventType
from online_processing.online_process import OnlineProcess


class TestPhoneUpdateEventProcessor(unittest.TestCase):
    @patch("online_processing.PhoneUpdateEvent.PhoneUpdateEvent")
    def test_handle(self, mock_phone_update_event):
        mock_json_data = '{"account": "user123", "phone": "0525815326"}'
        event_processor = eventType.PhoneUpdateEventProcessor(mock_json_data)

        # Set up mock expectations for PhoneUpdateEvent
        mock_event_instance = mock_phone_update_event.return_value
        event_processor.handle()

        # Assert that PhoneUpdateEvent.activate_all() was called
        mock_event_instance.activate_all.assert_called_once()


class TestPhoneUpdateEvent(unittest.TestCase):
    def test_phoneUsedByAccount(self):
        mock_json_data = '{"account": "user123", "phone": "0525815326"}'
        event = eventType.PhoneUpdateEvent(mock_json_data)

        with patch.object(OnlineProcess, "write_to_kafka") as mock_write_to_kafka:
            event.activate_all()

        mock_write_to_kafka.assert_called_once_with(
            producer_topic="demo_cons", output=mock_json_data
        )


if __name__ == "__main__":
    unittest.main()
