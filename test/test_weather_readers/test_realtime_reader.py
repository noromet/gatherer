"""
Unit tests for RealtimeReader.
"""

import unittest
from unittest.mock import patch
import uuid
from weather_readers.realtime_reader import RealtimeReader
from schema import WeatherStation


class TestRealtimeReader(unittest.TestCase):
    """
    Test cases for RealtimeReader.get_data method.
    """

    @patch(
        "weather_readers.realtime_reader.RealtimeReader.fetch_data",
        return_value={"live": {}},
    )
    def test_get_data_good(self, _):
        """
        Test the get_data method of RealtimeReader with valid data.
        """
        station = WeatherStation(
            ws_id=uuid.uuid4(),
            connection_type="realtime",
            field1="http://dummy.url",
            field2=None,
            field3=None,
            pressure_offset=0.0,
            data_timezone="Etc/UTC",
            local_timezone="Etc/UTC",
        )
        reader = RealtimeReader()
        record = reader.get_data(station)
        self.assertIsNotNone(record)


if __name__ == "__main__":
    unittest.main()
