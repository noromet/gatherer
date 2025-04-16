"""
Unit tests for WeatherlinkV2Reader.
"""

import unittest
from unittest.mock import patch
import uuid
from weather_readers.weatherlink_v2_reader import WeatherlinkV2Reader
from schema import WeatherStation


class TestWeatherlinkV2Reader(unittest.TestCase):
    """
    Test cases for WeatherlinkV2Reader.get_data method.
    """

    @patch(
        "weather_readers.weatherlink_v2_reader.WeatherlinkV2Reader.fetch_data",
        return_value={"live": {}, "daily": {}},
    )
    def test_get_data_good(self, _):
        """
        Test the get_data method of WeatherlinkV2Reader with valid data.
        """
        station = WeatherStation(
            ws_id=uuid.uuid4(),
            connection_type="weatherlink_v2",
            field1="id",
            field2="key",
            field3="secret",
            pressure_offset=0.0,
            data_timezone="Etc/UTC",
            local_timezone="Etc/UTC",
        )
        reader = WeatherlinkV2Reader("placeholder", "placeholder")
        record = reader.get_data(station)
        self.assertIsNotNone(record)


if __name__ == "__main__":
    unittest.main()
