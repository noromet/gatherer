"""
Unit tests for EcowittReader.
"""

import unittest
from unittest.mock import patch

from test.factories import create_weather_reader, create_weather_station

from weather_readers.ecowitt_reader import EcowittReader


class TestEcowittReader(unittest.TestCase):
    """
    Test cases for EcowittReader.get_data method.
    """

    @patch(
        "weather_readers.ecowitt_reader.EcowittReader.fetch_data",
        return_value={"live": {}, "daily": {}},
    )
    def test_get_data_good(self, _):
        """
        Test the get_data method of EcowittReader with valid data.
        """
        station = create_weather_station(connection_type="ecowitt")
        reader = create_weather_reader(EcowittReader)

        record = reader.get_data(station)
        self.assertIsNotNone(record)
        # self.assertAlmostEqual(record.temperature, 16)  # Uncomment and adjust as needed


if __name__ == "__main__":
    unittest.main()
