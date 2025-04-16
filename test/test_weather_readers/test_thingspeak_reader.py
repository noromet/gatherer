"""
Unit tests for ThingspeakReader.
"""

import unittest
from unittest.mock import patch
import uuid
from weather_readers.thingspeak_reader import ThingspeakReader
from schema import WeatherStation


class TestThingspeakReader(unittest.TestCase):
    """
    Test cases for ThingspeakReader.get_data method.
    """

    @patch(
        "weather_readers.thingspeak_reader.ThingspeakReader.fetch_data",
        return_value={"live": {}},
    )
    def test_get_data_good(self, _):
        """
        Test the get_data method of ThingspeakReader with valid data.
        """
        station = WeatherStation(
            ws_id=uuid.uuid4(),
            connection_type="thingspeak",
            field1="channelid",
            field2=None,
            field3=None,
            pressure_offset=0.0,
            data_timezone="Etc/UTC",
            local_timezone="Etc/UTC",
        )
        reader = ThingspeakReader("placeholder")
        record = reader.get_data(station)
        self.assertIsNotNone(record)
        # self.assertAlmostEqual(record.temperature, 21.5)  # Uncomment and adjust as needed


if __name__ == "__main__":
    unittest.main()
