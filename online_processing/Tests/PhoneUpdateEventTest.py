import unittest
from unittest.mock import patch, mock_open, MagicMock
import online_processing.PhoneUpdatedEvent as eventType
from online_processing.online_process import OnlineProcess


class TestEmailUpdateEvent(unittest.TestCase):
    def test_emailUsedByAccount(self):
        mock_json_data = '{"account": "user123", "phone": "0525815326"}'
        event = eventType.PhoneUpdateEvent(mock_json_data)

        with patch.object(OnlineProcess, "write_to_kafka") as mock_write_to_kafka:
            event.activate_all()

        mock_write_to_kafka.assert_called_once_with(
            producer_topic="demo_cons", output=mock_json_data
        )


if __name__ == "__main__":
    unittest.main()
