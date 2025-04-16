"""
Unit tests for WeatherLinkV1Reader.
"""

import unittest
from unittest.mock import patch
import uuid
from weather_readers.weatherlink_v1_reader import WeatherLinkV1Reader
from schema import WeatherStation


class TestWeatherLinkV1Reader(unittest.TestCase):
    """
    Test cases for WeatherLinkV1Reader.get_data method.
    """

    @patch(
        "weather_readers.weatherlink_v1_reader.WeatherLinkV1Reader.fetch_data",
        return_value={"live": {}, "daily": {}},
    )
    def test_get_data_good(self, _):
        """
        Test the get_data method of WeatherLinkV1Reader with valid data.
        """
        station = WeatherStation(
            ws_id=uuid.uuid4(),
            connection_type="weatherlink_v1",
            field1="user",
            field2="token",
            field3="pass",
            pressure_offset=0.0,
            data_timezone="Etc/UTC",
            local_timezone="Etc/UTC",
        )
        reader = WeatherLinkV1Reader("placeholder")
        record = reader.get_data(station)
        self.assertIsNotNone(record)
        # self.assertAlmostEqual(record.temperature, 18.5)  # Uncomment and adjust as needed


if __name__ == "__main__":
    unittest.main()
