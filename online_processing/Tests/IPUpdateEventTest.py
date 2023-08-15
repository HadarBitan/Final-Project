import unittest
from unittest.mock import patch, mock_open, MagicMock
import online_processing.IPUpdatedEvent as eventType
from online_processing.online_process import OnlineProcess


class TestEmailUpdateEvent(unittest.TestCase):
    def test_emailUsedByAccount(self):
        mock_json_data = '{"account": "user123", "IP": "123.12.0.25"}'
        event = eventType.IPUpdateEvent(mock_json_data)

        with patch.object(OnlineProcess, "write_to_kafka") as mock_write_to_kafka:
            event.activate_all()

        mock_write_to_kafka.assert_called_once_with(
            producer_topic="demo_cons", output=mock_json_data
        )


if __name__ == "__main__":
    unittest.main()
