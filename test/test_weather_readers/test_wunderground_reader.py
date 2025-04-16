"""
Unit tests for WundergroundReader.
"""

import unittest
from unittest.mock import patch
import uuid
from weather_readers.wunderground_reader import WundergroundReader
from schema import WeatherStation


class TestWundergroundReader(unittest.TestCase):
    """
    Test cases for WundergroundReader.get_data method.
    """

    @patch(
        "weather_readers.wunderground_reader.WundergroundReader.fetch_data",
        return_value={"live": {}, "daily": {}},
    )
    def test_get_data_good(self, _):
        """
        Test the get_data method of WundergroundReader with valid data.
        """
        station = WeatherStation(
            ws_id=uuid.uuid4(),
            connection_type="wunderground",
            field1="stationid",
            field2="token",
            field3=None,
            pressure_offset=0.0,
            data_timezone="Etc/UTC",
            local_timezone="Etc/UTC",
        )
        reader = WundergroundReader("placeholder", "placeholder")
        record = reader.get_data(station)
        self.assertIsNotNone(record)
        # self.assertEqual(record.temperature, 19)  # Uncomment and adjust as needed


if __name__ == "__main__":
    unittest.main()
