"""
Unit tests for MeteoclimaticReader.
"""

import unittest
from unittest.mock import patch
import uuid
from weather_readers.meteoclimatic_reader import MeteoclimaticReader
from schema import WeatherStation


class TestMeteoclimaticReader(unittest.TestCase):
    """
    Test cases for MeteoclimaticReader.get_data method.
    """

    @patch(
        "weather_readers.meteoclimatic_reader.MeteoclimaticReader.fetch_data",
        return_value={"live": {}},
    )
    def test_get_data_good(self, _):
        """
        Test the get_data method of MeteoclimaticReader with valid data.
        """
        station = WeatherStation(
            ws_id=uuid.uuid4(),
            connection_type="meteoclimatic",
            field1="http://dummy.url",
            field2=None,
            field3=None,
            pressure_offset=0.0,
            data_timezone="Etc/UTC",
            local_timezone="Etc/UTC",
        )
        reader = MeteoclimaticReader()
        record = reader.get_data(station)
        self.assertIsNotNone(record)
        # self.assertEqual(record.temperature, 20)  # Uncomment and adjust as needed


if __name__ == "__main__":
    unittest.main()
