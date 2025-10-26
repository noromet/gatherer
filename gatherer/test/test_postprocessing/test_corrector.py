"""
Test cases for the Corrector class in the postprocessing module.
"""

import unittest

from gatherer.postprocessing.corrector import Corrector
from gatherer.test.factories import create_weather_record


class TestCorrector(unittest.TestCase):
    """
    Test cases for the Corrector class.
    """

    def setUp(self):
        """
        Set up the test case.
        """
        self.corrector = Corrector()
        self.record = create_weather_record()

    def test_apply_pressure_offset(self):
        """
        Test the apply_pressure_offset method with different offsets.
        """
        record_to_sum = create_weather_record(pressure=1013.0)
        result = self.corrector.apply_pressure_offset(record_to_sum, 5.0)
        self.assertEqual(result.pressure, 1018.0)

        record_to_subtract = create_weather_record(pressure=1013.0)
        result = self.corrector.apply_pressure_offset(record_to_subtract, -5.0)
        self.assertEqual(result.pressure, 1008.0)

        record_to_zero = create_weather_record(pressure=1013.0)
        result = self.corrector.apply_pressure_offset(record_to_zero, 0.0)
        self.assertEqual(result.pressure, 1013.0)

        # Test with None pressure
        record_none_pressure = create_weather_record(pressure=None)
        result = self.corrector.apply_pressure_offset(record_none_pressure, 5.0)
        self.assertIsNone(result.pressure)

        # Test with None offset
        record_none_offset = create_weather_record(pressure=1013.0)
        result = self.corrector.apply_pressure_offset(record_none_offset, None)
        self.assertEqual(result.pressure, 1013.0)

    def test_apply_rounding(self):
        """
        Test the apply_rounding method with different decimal places.
        """
        record = create_weather_record(
            temperature=25.123,
            wind_speed=10.456,
            max_wind_speed=20.789,
            wind_direction=180.0,
            rain=5.678,
            humidity=50.123,
            pressure=1013.456,
            cumulative_rain=10.987,
            max_temperature=30.654,
            min_temperature=20.321,
            wind_gust=15.432,
            max_wind_gust=25.876,
        )
        result = self.corrector.apply_rounding(record, decimals=1)
        self.assertEqual(result.temperature, 25.1)
        self.assertEqual(result.wind_speed, 10.5)
        self.assertEqual(result.max_wind_speed, 20.8)
        self.assertEqual(result.rain, 5.7)
        self.assertEqual(result.humidity, 50.1)
        self.assertEqual(result.pressure, 1013.5)
        self.assertEqual(result.cumulative_rain, 11.0)
        self.assertEqual(result.max_temperature, 30.7)
        self.assertEqual(result.min_temperature, 20.3)
        self.assertEqual(result.wind_gust, 15.4)
        self.assertEqual(result.max_wind_gust, 25.9)

        # Test with None values
        record_with_none = create_weather_record(
            temperature=None, wind_speed=None, humidity=None
        )
        result = self.corrector.apply_rounding(record_with_none, decimals=1)
        self.assertIsNone(result.temperature)
        self.assertIsNone(result.wind_speed)
        self.assertIsNone(result.humidity)

        # Test with different decimal precision
        record_for_precision = create_weather_record(temperature=25.5678)
        result = self.corrector.apply_rounding(record_for_precision, decimals=2)
        self.assertEqual(result.temperature, 25.57)

    def test_correct(self):
        """
        Test the correct method that applies both offset and rounding.
        """
        record = create_weather_record(
            temperature=25.123,
            pressure=1013.456,
        )

        # Apply both offset and rounding
        result = self.corrector.correct(record, offset=5.0, decimals=1)
        self.assertEqual(
            result.pressure, 1018.5
        )  # 1013.456 + 5.0 = 1018.456, rounded to 1018.5
        self.assertEqual(result.temperature, 25.1)  # 25.123 rounded to 25.1

        # Verify other fields are also rounded
        self.assertEqual(
            result.wind_speed, 10.0
        )  # Default value from create_weather_record
        self.assertEqual(result.humidity, 50.0)

        # Test with default parameters
        record_default = create_weather_record(
            temperature=25.123,
            pressure=1013.456,
        )
        result = self.corrector.correct(record_default)
        self.assertEqual(result.pressure, 1013.5)  # No offset applied, just rounding
        self.assertEqual(result.temperature, 25.1)


if __name__ == "__main__":
    unittest.main()
