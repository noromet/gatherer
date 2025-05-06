"""
Unit tests for WundergroundReader.
"""

import unittest
from unittest.mock import patch

from test.factories import create_weather_reader, create_weather_station
from test.test_weather_readers.base import WeatherReaderTestBase
from weather_readers.wunderground_reader import WundergroundReader


class TestWundergroundReader(WeatherReaderTestBase):
    """
    Test cases for WundergroundReader.read method.
    """

    live_fixture_filename = "wunderground_live_response.json"
    daily_fixture_filename = "wunderground_daily_response.json"

    @patch("weather_readers.wunderground_reader.WundergroundReader.fetch_data")
    def test_read_good(self, mock_fetch_data):
        """
        Test the read method of WundergroundReader with valid data.
        """
        mock_fetch_data.return_value = self.test_data

        station = create_weather_station(connection_type="thingspeak")
        reader = create_weather_reader(WundergroundReader)

        record = reader.read(station)

        self.assertIsNotNone(record)

        # live
        self.assertEqual(record.temperature, 5)
        self.assertEqual(record.humidity, 79)
        self.assertEqual(record.rain, 0)
        self.assertEqual(record.pressure, 1014.6)
        self.assertEqual(record.wind_speed, 5.4)
        self.assertEqual(record.wind_gust, 9.4)
        self.assertEqual(record.wind_direction, 15)

        # daily
        self.assertEqual(record.cumulative_rain, 3.3)
        self.assertEqual(record.max_wind_speed, 30.2)
        self.assertEqual(record.max_wind_gust, 36.7)
        self.assertEqual(record.max_temperature, 6.4)
        self.assertEqual(record.min_temperature, 0.8)


if __name__ == "__main__":
    unittest.main()
