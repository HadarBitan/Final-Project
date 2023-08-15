import unittest
from unittest.mock import patch, mock_open, MagicMock
import online_processing.EmailUpdateEvent as eventType
from online_processing.online_process import OnlineProcess


# class TestEmailUpdateEventProcessor(unittest.TestCase):
#     @patch("online_processing.online_process.OnlineProcess")
#     def test_handle(self, mock_online_process):
#         mock_json_data = '{"account": "user123", "email": "newemail@example.com"}'
#
#         with patch("builtins.open", mock_open(read_data=mock_json_data)):
#             event_processor = eventType.EmailUpdateEventProcessor(mock_json_data)
#
#             with patch.object(OnlineProcess, "write_to_kafka") as mock_write_to_kafka:
#                 event_processor.handle()
#
#             # event_processor.handle()
#             #
#             # mock_online_process_instance = mock_online_process.return_value
#             mock_write_to_kafka.write_to_kafka.assert_called_once_with(
#                 producer_topic="demo_cons", output=mock_json_data
#             )
class TestEmailUpdateEventProcessor(unittest.TestCase):
    def test_handle(self):
        mock_json_data = '{"account": "user123", "email": "newemail@example.com"}'
        # Create a mock EventProcessor instance and override the json_data
        event_processor = eventType.EmailUpdateEventProcessor(mock_json_data)

        # Mock the EmailUpdateEvent.activate_all() method
        mock_event = MagicMock()
        event_processor.EmailUpdateEvent = mock_event
        event_processor.handle()

        # Assert that EmailUpdateEvent.activate_all() was called
        mock_event.assert_called_once_with({"account": "user123", "email": "new@example.com"})


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
