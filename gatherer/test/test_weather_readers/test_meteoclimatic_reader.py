"""
Unit tests for MeteoclimaticReader.
"""

import unittest
from unittest.mock import patch

from gatherer.test.factories import create_weather_reader, create_weather_station
from gatherer.test.test_weather_readers.base import WeatherReaderTestBase
from gatherer.weather_readers.meteoclimatic_reader import MeteoclimaticReader


class TestMeteoclimaticReader(WeatherReaderTestBase):
    """
    Test cases for MeteoclimaticReader.read method.
    """

    live_fixture_filename = "meteoclimatic_response.txt"

    @patch(
        "gatherer.weather_readers.meteoclimatic_reader.MeteoclimaticReader.fetch_data"
    )
    def test_read_good(self, mock_fetch_data):
        """
        Test the read method of MeteoclimaticReader with valid data.
        """
        mock_fetch_data.return_value = self.test_data

        station = create_weather_station(connection_type="meteoclimatic")
        reader = create_weather_reader(MeteoclimaticReader)

        record = reader.read(station)

        self.assertIsNotNone(record)

        # live
        self.assertEqual(record.temperature, 16.8)
        self.assertEqual(record.humidity, 70)
        self.assertEqual(record.rain, None)  # meteoclimatic does not provide rain rate
        self.assertEqual(record.pressure, 1018.8)
        self.assertEqual(record.wind_speed, 14)
        self.assertEqual(
            record.wind_gust, None
        )  # meteoclimatic does not provide wind gust
        self.assertEqual(record.wind_direction, 270)

        # daily
        self.assertEqual(record.cumulative_rain, 0)
        self.assertEqual(
            record.max_wind_speed, None
        )  # meteoclimatic does not provide max wind speed
        self.assertEqual(record.max_wind_gust, 37)
        self.assertEqual(record.max_temperature, 19.2)
        self.assertEqual(record.min_temperature, 9.2)


if __name__ == "__main__":
    unittest.main()
