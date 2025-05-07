"""
Unit tests for WeatherlinkV2Reader.
"""

import unittest
from unittest.mock import patch

from gatherer.test.factories import create_weather_reader, create_weather_station
from gatherer.test.test_weather_readers.base import WeatherReaderTestBase
from gatherer.weather_readers.weatherlink_v2_reader import WeatherlinkV2Reader


class TestWeatherlinkV2Reader(WeatherReaderTestBase):
    """
    Test cases for WeatherlinkV2Reader.read method.
    """

    live_fixture_filename = "weatherlink_v2_response.json"

    @patch(
        "gatherer.weather_readers.weatherlink_v2_reader.WeatherlinkV2Reader.fetch_data"
    )
    def test_read_good(self, mock_fetch_data):
        """
        Test the read method of WeatherlinkV2Reader with valid data.
        """
        mock_fetch_data.return_value = self.test_data

        station = create_weather_station(connection_type="weatherlink_v2")
        reader = create_weather_reader(WeatherlinkV2Reader)

        record = reader.read(station)

        self.assertIsNotNone(record)

        # live
        self.assertEqual(record.temperature, 19.2)
        self.assertEqual(record.humidity, 33)
        self.assertEqual(record.rain, 0)
        self.assertEqual(record.pressure, 1010.5)
        self.assertEqual(record.wind_speed, 11.3)
        self.assertEqual(record.wind_gust, 35.4)
        self.assertEqual(record.wind_direction, 292)

        # daily
        self.assertEqual(record.cumulative_rain, 0)
        self.assertEqual(record.max_wind_speed, None)
        self.assertEqual(record.max_wind_gust, None)
        self.assertEqual(record.max_temperature, None)
        self.assertEqual(record.min_temperature, None)


if __name__ == "__main__":
    unittest.main()
