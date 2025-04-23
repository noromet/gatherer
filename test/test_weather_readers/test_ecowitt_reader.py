"""
Unit tests for EcowittReader.
"""

import unittest
from unittest.mock import patch

from test.factories import create_weather_reader, create_weather_station
from test.test_weather_readers.base import WeatherReaderTestBase
from weather_readers.ecowitt_reader import EcowittReader


class TestEcowittReader(WeatherReaderTestBase):
    """
    Test cases for EcowittReader.read method.
    """

    # Using new structure with separate live and daily files
    live_fixture_filename = "ecowitt_live_response.json"
    daily_fixture_filename = "ecowitt_daily_response.json"

    @patch("weather_readers.ecowitt_reader.EcowittReader.fetch_data")
    def test_read_good(self, mock_fetch_data):
        """
        Test the read method of EcowittReader with valid data.
        """
        mock_fetch_data.return_value = self.test_data

        station = create_weather_station(connection_type="ecowitt")
        reader = create_weather_reader(
            EcowittReader, live_endpoint="", daily_endpoint=""
        )

        record = reader.read(station)

        self.assertIsNotNone(record)

        # live
        self.assertEqual(record.temperature, 12.3)
        self.assertEqual(record.humidity, 61)
        self.assertEqual(record.rain, 1.2)
        self.assertEqual(record.pressure, None)
        self.assertEqual(record.wind_speed, 2.2)
        self.assertEqual(record.wind_gust, 5.4)
        self.assertEqual(record.wind_direction, 304)

        # daily
        self.assertEqual(record.cumulative_rain, 7.1)
        self.assertEqual(record.max_wind_speed, 18.7)
        self.assertEqual(record.max_wind_gust, 27.7)
        self.assertEqual(record.max_temperature, 14.7)
        self.assertEqual(record.min_temperature, 5.0)


if __name__ == "__main__":
    unittest.main()
