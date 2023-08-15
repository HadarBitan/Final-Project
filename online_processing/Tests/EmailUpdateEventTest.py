import unittest
from unittest.mock import patch
import online_processing.EmailUpdateEvent as eventType
from online_processing.online_process import OnlineProcess


class TestEmailUpdateEventProcessor(unittest.TestCase):
    @patch("online_processing.EmailUpdateEvent.EmailUpdateEvent")
    def test_handle(self, mock_email_update_event):
        mock_json_data = '{"account": "user123", "email": "newemail@example.com"}'
        event_processor = eventType.EmailUpdateEventProcessor(mock_json_data)

        # Set up mock expectations for EmailUpdateEvent
        mock_event_instance = mock_email_update_event.return_value
        event_processor.handle()

        # Assert that EmailUpdateEvent.activate_all() was called
        mock_event_instance.activate_all.assert_called_once()


class TestEmailUpdateEvent(unittest.TestCase):
    def test_emailUsedByAccount(self):
        mock_json_data = '{"account": "user123", "email": "newemail@example.com"}'
        event = eventType.EmailUpdateEvent(mock_json_data)

        with patch.object(OnlineProcess, "write_to_kafka") as mock_write_to_kafka:
            event.activate_all()

        mock_write_to_kafka.assert_called_once_with(
            producer_topic="demo_cons", output=mock_json_data
        )


if __name__ == "__main__":
    unittest.main()
