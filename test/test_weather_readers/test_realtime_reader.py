"""
Unit tests for RealtimeReader.
"""

import unittest
from unittest.mock import patch

from test.factories import create_weather_reader, create_weather_station
from test.test_weather_readers.base import WeatherReaderTestBase
from weather_readers.realtime_reader import RealtimeReader


class TestRealtimeReader(WeatherReaderTestBase):
    """
    Test cases for RealtimeReader.read method.
    """

    live_fixture_filename = "realtime_response.txt"

    @patch("weather_readers.realtime_reader.RealtimeReader.fetch_data")
    def test_read_good(self, mock_fetch_data):
        """
        Test the read method of RealtimeReader with valid data.
        """
        mock_fetch_data.return_value = self.test_data

        station = create_weather_station(connection_type="realtime")
        reader = create_weather_reader(RealtimeReader)

        record = reader.read(station)

        self.assertIsNotNone(record)

        # live
        self.assertEqual(record.temperature, 12.0)
        self.assertEqual(record.humidity, 85)
        self.assertEqual(record.rain, 0.0)
        self.assertEqual(record.pressure, 1018.6)
        self.assertEqual(record.wind_speed, 16.8)
        self.assertEqual(record.wind_gust, None)  # realtime does not provide wind gust
        self.assertEqual(record.wind_direction, 231)

        # daily
        self.assertEqual(record.cumulative_rain, 0.1)
        self.assertEqual(record.max_wind_speed, 49.7)
        self.assertEqual(record.max_wind_gust, None)
        self.assertEqual(record.max_temperature, 31.3)
        self.assertEqual(record.min_temperature, 3.8)


if __name__ == "__main__":
    unittest.main()
