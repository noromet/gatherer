"""
Unit tests for HolfuyReader.
"""

import unittest
from unittest.mock import patch
import uuid
from weather_readers.holfuy_reader import HolfuyReader
from schema import WeatherStation


class TestHolfuyReader(unittest.TestCase):
    """
    Test cases for HolfuyReader.get_data method.
    """

    @patch(
        "weather_readers.holfuy_reader.HolfuyReader.fetch_data",
        return_value={"live": {}, "daily": {}},
    )
    def test_get_data_good(self, _):
        """
        Test the get_data method of HolfuyReader with valid data.
        """
        station = WeatherStation(
            ws_id=uuid.uuid4(),
            connection_type="holfuy",
            field1="stationid",
            field2=None,
            field3="password",
            pressure_offset=0.0,
            data_timezone="Etc/UTC",
            local_timezone="Etc/UTC",
        )
        reader = HolfuyReader("placeholder", "placeholder")
        record = reader.get_data(station)
        self.assertIsNotNone(record)
        # self.assertAlmostEqual(record.temperature, 17.5)  # Uncomment and adjust as needed


if __name__ == "__main__":
    unittest.main()
