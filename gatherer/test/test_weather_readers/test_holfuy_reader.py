"""
Unit tests for HolfuyReader.
"""

import unittest
from unittest.mock import patch

from gatherer.test.factories import create_weather_reader, create_weather_station
from gatherer.test.test_weather_readers.base import WeatherReaderTestBase
from gatherer.weather_readers.holfuy_reader import HolfuyReader


class TestHolfuyReader(WeatherReaderTestBase):
    """
    Test cases for HolfuyReader.read method.
    """

    live_fixture_filename = "holfuy_live_response.json"
    daily_fixture_filename = "holfuy_daily_response.json"

    @patch("gatherer.weather_readers.holfuy_reader.HolfuyReader.fetch_data")
    def test_read_good(self, mock_fetch_data):
        """
        Test the read method of HolfuyReader with valid data.
        """
        mock_fetch_data.return_value = self.test_data

        station = create_weather_station(connection_type="ecowitt")
        reader = create_weather_reader(
            HolfuyReader, live_endpoint="", daily_endpoint=""
        )

        record = reader.read(station)

        self.assertIsNotNone(record)

        # live
        self.assertEqual(record.temperature, 5.4)
        self.assertEqual(record.humidity, 93.7)
        self.assertEqual(record.rain, 0.0)
        self.assertEqual(record.pressure, 1014)
        self.assertEqual(record.wind_speed, 5)
        self.assertEqual(record.wind_gust, 13)
        self.assertEqual(record.wind_direction, 203)

        # daily
        self.assertEqual(record.cumulative_rain, 10.0)
        self.assertEqual(record.max_wind_speed, 5)
        self.assertEqual(record.max_wind_gust, 13)
        self.assertEqual(record.max_temperature, 8.4)
        self.assertEqual(record.min_temperature, 0.1)


if __name__ == "__main__":
    unittest.main()
