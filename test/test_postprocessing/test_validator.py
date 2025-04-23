"""
Test cases for the Validator class in the postprocessing module.
"""

import unittest
from test.factories import create_weather_record
from postprocessing.validator import Validator, WEATHER_SAFE_RANGES


class TestValidator(unittest.TestCase):
    """
    Test cases for the Validator class.
    """

    def setUp(self):
        """
        Set up the test case.
        """
        self.validator = Validator()
        self.record = create_weather_record()

    def test_validate_with_valid_data(self):
        """
        Test the validate method with valid data.
        """
        record = create_weather_record()
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)

        self.assertEqual(result.temperature, record.temperature)
        self.assertEqual(result.wind_speed, record.wind_speed)
        self.assertEqual(result.humidity, record.humidity)
        self.assertEqual(result.pressure, record.pressure)

    def test_temperature_validation(self):
        """
        Test the temperature validation.
        """
        temp_min, temp_max = WEATHER_SAFE_RANGES["temperature"]

        # Test below minimum
        record_temp_below_minimum = create_weather_record(temperature=temp_min - 1)
        result = self.validator.validate(record_temp_below_minimum)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.temperature)

        # Test above maximum
        record_temp_above_maximum = create_weather_record(temperature=temp_max + 1)
        result = self.validator.validate(record_temp_above_maximum)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.temperature)

        # Test valid value
        record_temp_valid = create_weather_record(temperature=(temp_min + temp_max) / 2)
        result = self.validator.validate(record_temp_valid)
        self.assertFalse(result.flagged)
        self.assertEqual(result.temperature, record_temp_valid.temperature)

    def test_wind_speed_validation(self):
        """
        Test the wind speed validation.
        """
        speed_min, speed_max = WEATHER_SAFE_RANGES["wind_speed"]

        # Test below minimum
        record_wind_speed_below_minimum = create_weather_record(
            wind_speed=speed_min - 1
        )
        result = self.validator.validate(record_wind_speed_below_minimum)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.wind_speed)

        # Test above maximum
        record_wind_speed_above_maximum = create_weather_record(
            wind_speed=speed_max + 1
        )
        result = self.validator.validate(record_wind_speed_above_maximum)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.wind_speed)

        # Test valid value
        record_wind_speed_valid = create_weather_record(
            wind_speed=(speed_min + speed_max) / 2
        )
        result = self.validator.validate(record_wind_speed_valid)
        self.assertFalse(result.flagged)
        self.assertEqual(result.wind_speed, record_wind_speed_valid.wind_speed)

    def test_humidity_validation(self):
        """
        Test the humidity validation.
        """
        humidity_min, humidity_max = WEATHER_SAFE_RANGES["humidity"]

        # Test below minimum
        record_humidity_below_minimum = create_weather_record(humidity=humidity_min - 1)
        result = self.validator.validate(record_humidity_below_minimum)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.humidity)

        # Test above maximum
        record_humidity_above_maximum = create_weather_record(humidity=humidity_max + 1)
        result = self.validator.validate(record_humidity_above_maximum)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.humidity)

        # Test valid value
        record_humidity_valid = create_weather_record(
            humidity=(humidity_min + humidity_max) / 2
        )
        result = self.validator.validate(record_humidity_valid)
        self.assertFalse(result.flagged)
        self.assertEqual(result.humidity, record_humidity_valid.humidity)

    def test_pressure_validation(self):
        """
        Test the pressure validation.
        """
        pressure_min, pressure_max = WEATHER_SAFE_RANGES["pressure"]

        # Test below minimum
        record_pressure_below_minimum = create_weather_record(pressure=pressure_min - 1)
        result = self.validator.validate(record_pressure_below_minimum)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.pressure)

        # Test above maximum
        record_pressure_above_maximum = create_weather_record(pressure=pressure_max + 1)
        result = self.validator.validate(record_pressure_above_maximum)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.pressure)

        # Test valid value
        record_pressure_valid = create_weather_record(
            pressure=(pressure_min + pressure_max) / 2
        )
        result = self.validator.validate(record_pressure_valid)
        self.assertFalse(result.flagged)
        self.assertEqual(result.pressure, record_pressure_valid.pressure)

    def test_wind_direction_validation(self):
        """
        Test the wind direction validation.
        """
        direction_min, direction_max = WEATHER_SAFE_RANGES["wind_direction"]

        # Test below minimum
        record_wind_direction_below_minimum = create_weather_record(
            wind_direction=direction_min - 1
        )
        result = self.validator.validate(record_wind_direction_below_minimum)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.wind_direction)

        # Test above maximum
        record_wind_direction_above_maximum = create_weather_record(
            wind_direction=direction_max + 1
        )
        result = self.validator.validate(record_wind_direction_above_maximum)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.wind_direction)

        # Test valid value
        record_wind_direction_valid = create_weather_record(
            wind_direction=(direction_min + direction_max) / 2
        )
        result = self.validator.validate(record_wind_direction_valid)
        self.assertFalse(result.flagged)
        self.assertEqual(
            result.wind_direction, record_wind_direction_valid.wind_direction
        )

    def test_rain_validation(self):
        """
        Test the rain validation.
        """
        rain_min, rain_max = WEATHER_SAFE_RANGES["rain"]

        # Test below minimum
        record_rain_below_minimum = create_weather_record(rain=rain_min - 1)
        result = self.validator.validate(record_rain_below_minimum)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.rain)

        # Test above maximum
        record_rain_above_maximum = create_weather_record(rain=rain_max + 1)
        result = self.validator.validate(record_rain_above_maximum)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.rain)

        # Test valid value
        record_rain_valid = create_weather_record(rain=(rain_min + rain_max) / 2)
        result = self.validator.validate(record_rain_valid)
        self.assertFalse(result.flagged)
        self.assertEqual(result.rain, record_rain_valid.rain)

    def test_cumulative_rain_validation(self):
        """
        Test the cumulative rain validation.
        """
        cum_rain_min, cum_rain_max = WEATHER_SAFE_RANGES["cumulative_rain"]

        # Test below minimum
        record_cum_rain_below_minimum = create_weather_record(
            cumulative_rain=cum_rain_min - 1
        )
        result = self.validator.validate(record_cum_rain_below_minimum)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.cumulative_rain)

        # Test above maximum
        record_cum_rain_above_maximum = create_weather_record(
            cumulative_rain=cum_rain_max + 1
        )
        result = self.validator.validate(record_cum_rain_above_maximum)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.cumulative_rain)

        # Test valid value
        record_cum_rain_valid = create_weather_record(
            cumulative_rain=(cum_rain_min + cum_rain_max) / 2
        )
        result = self.validator.validate(record_cum_rain_valid)
        self.assertFalse(result.flagged)
        self.assertEqual(result.cumulative_rain, record_cum_rain_valid.cumulative_rain)

    def test_max_temperature_validation(self):
        """
        Test the max temperature validation.
        """
        temp_min, temp_max = WEATHER_SAFE_RANGES["max_temperature"]

        # Test below minimum
        record = create_weather_record(max_temperature=temp_min - 1)
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.max_temperature)

        # Test above maximum
        record = create_weather_record(max_temperature=temp_max + 1)
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.max_temperature)

        # Test valid value
        record = create_weather_record(max_temperature=(temp_min + temp_max) / 2)
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)
        self.assertEqual(result.max_temperature, record.max_temperature)

    def test_min_temperature_validation(self):
        """
        Test the min temperature validation.
        """
        temp_min, temp_max = WEATHER_SAFE_RANGES["min_temperature"]

        # Test below minimum
        record = create_weather_record(min_temperature=temp_min - 1)
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.min_temperature)

        # Test above maximum
        record = create_weather_record(min_temperature=temp_max + 1)
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.min_temperature)

        # Test valid value
        record = create_weather_record(min_temperature=(temp_min + temp_max) / 2)
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)
        self.assertEqual(result.min_temperature, record.min_temperature)

    def test_wind_gust_validation(self):
        """
        Test the wind gust validation.
        """
        gust_min, gust_max = WEATHER_SAFE_RANGES["wind_gust"]

        # Test below minimum
        record = create_weather_record(wind_gust=gust_min - 1)
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.wind_gust)

        # Test above maximum
        record = create_weather_record(wind_gust=gust_max + 1)
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.wind_gust)

        # Test valid value
        record = create_weather_record(wind_gust=(gust_min + gust_max) / 2)
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)
        self.assertEqual(result.wind_gust, record.wind_gust)

    def test_max_wind_gust_validation(self):
        """
        Test the max wind gust validation.
        """
        gust_min, gust_max = WEATHER_SAFE_RANGES["max_wind_gust"]

        # Test below minimum
        record = create_weather_record(max_wind_gust=gust_min - 1)
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.max_wind_gust)

        # Test above maximum
        record = create_weather_record(max_wind_gust=gust_max + 1)
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.max_wind_gust)

        # Test valid value
        record = create_weather_record(max_wind_gust=(gust_min + gust_max) / 2)
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)
        self.assertEqual(result.max_wind_gust, record.max_wind_gust)

    def test_validate_multiple_invalid_fields(self):
        """
        Test validation when multiple fields are invalid.
        """
        record = create_weather_record(
            temperature=-100, humidity=150, pressure=900  # Invalid  # Invalid  # Valid
        )
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)
        self.assertIsNone(result.temperature)
        self.assertIsNone(result.humidity)
        self.assertEqual(result.pressure, 900)  # Valid value preserved


if __name__ == "__main__":
    unittest.main()
