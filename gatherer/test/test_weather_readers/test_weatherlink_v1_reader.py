"""
Unit tests for WeatherlinkV1Reader.
"""

import unittest
from unittest.mock import patch

from gatherer.test.factories import create_weather_reader, create_weather_station
from gatherer.test.test_weather_readers.base import WeatherReaderTestBase
from gatherer.weather_readers.weatherlink_v1_reader import WeatherlinkV1Reader


class TestWeatherlinkV1Reader(WeatherReaderTestBase):
    """
    Test cases for WeatherlinkV1Reader.read method.
    """

    live_fixture_filename = "weatherlink_v1_response.json"

    @patch(
        "gatherer.weather_readers.weatherlink_v1_reader.WeatherlinkV1Reader.fetch_data"
    )
    def test_read_good(self, mock_fetch_data):
        """
        Test the read method of WeatherlinkV1Reader with valid data.
        """
        mock_fetch_data.return_value = self.test_data

        station = create_weather_station(connection_type="weatherlink_v1")
        reader = create_weather_reader(WeatherlinkV1Reader)

        record = reader.read(station)

        self.assertIsNotNone(record)

        # live
        self.assertEqual(record.temperature, 6.0)
        self.assertEqual(record.humidity, 81)
        self.assertEqual(record.rain, 25.4)
        self.assertEqual(record.pressure, 1012)
        self.assertEqual(record.wind_speed, 6.4)
        self.assertEqual(record.wind_gust, None)
        self.assertEqual(record.wind_direction, 20)

        # daily
        self.assertEqual(record.cumulative_rain, 28.9)
        self.assertEqual(record.max_wind_speed, 30.6)
        self.assertEqual(record.max_wind_gust, None)
        self.assertEqual(record.max_temperature, 11.0)
        self.assertEqual(record.min_temperature, -5.1)


if __name__ == "__main__":
    unittest.main()
