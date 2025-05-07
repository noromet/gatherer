"""
Unit tests for ThingspeakReader.
"""

import unittest
from unittest.mock import patch

from gatherer.test.factories import create_weather_reader, create_weather_station
from gatherer.test.test_weather_readers.base import WeatherReaderTestBase
from gatherer.weather_readers.thingspeak_reader import ThingspeakReader


class TestThingspeakReader(WeatherReaderTestBase):
    """
    Test cases for ThingspeakReader.read method.
    """

    live_fixture_filename = "thingspeak_response.json"

    @patch("gatherer.weather_readers.thingspeak_reader.ThingspeakReader.fetch_data")
    def test_read_good(self, mock_fetch_data):
        """
        Test the read method of ThingspeakReader with valid data.
        """
        mock_fetch_data.return_value = self.test_data

        station = create_weather_station(connection_type="thingspeak")
        reader = create_weather_reader(ThingspeakReader)

        record = reader.read(station)

        self.assertIsNotNone(record)

        # live
        self.assertEqual(record.temperature, 10.49)
        self.assertEqual(record.humidity, 54.43)
        self.assertEqual(record.rain, None)  # Not present in JSON
        self.assertEqual(record.pressure, 921.6)
        self.assertEqual(record.wind_speed, None)  # Not present in JSON
        self.assertEqual(record.wind_gust, None)  # Not present in JSON
        self.assertEqual(record.wind_direction, None)  # Not present in JSON

        # daily
        self.assertEqual(record.cumulative_rain, None)  # Not present in JSON
        self.assertEqual(record.max_wind_speed, None)  # Not present in JSON
        self.assertEqual(record.max_wind_gust, None)  # Not present in JSON
        self.assertEqual(record.max_temperature, None)  # Not present in JSON
        self.assertEqual(record.min_temperature, None)  # Not present in JSON


if __name__ == "__main__":
    unittest.main()
