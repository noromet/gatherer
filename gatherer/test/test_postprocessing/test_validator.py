"""
Test cases for the Validator class in the postprocessing module.
"""

import unittest
from gatherer.test.factories import create_weather_record
from gatherer.postprocessing.validator import Validator, WEATHER_SAFE_RANGES


class TestValidatorRanges(unittest.TestCase):
    """
    Test cases for the Validator class range validation.
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
        record_wind_speed_valid = (
            create_weather_record(  # the values must be incremental in this order
                wind_speed=(speed_min + speed_max) / 2,
                wind_gust=(speed_min + speed_max) / 1.5,
                max_wind_speed=(speed_min + speed_max) / 1.2,
                max_wind_gust=(speed_min + speed_max) / 1.2,
            )
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
        record = create_weather_record(
            min_temperature=temp_min,
            temperature=(temp_min + temp_max) / 2,
            max_temperature=temp_max,
        )
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
        record = create_weather_record(
            wind_speed=(gust_min + gust_max) / 2.1,
            max_wind_speed=(gust_min + gust_max) / 2,
            wind_gust=(gust_min + gust_max) / 2,
            max_wind_gust=(gust_min + gust_max) / 1.5,
        )

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


class TestValidatorConsistency(unittest.TestCase):
    """
    Test cases for the Validator class consistency.
    """

    def setUp(self):
        """
        Set up the test case.
        """
        self.validator = Validator()
        self.record = create_weather_record()

    def test_temperature_consistency_all_values(self):
        """
        Test temperature consistency validation with all three values present.
        Test ensures that min_temperature <= temperature <= max_temperature.
        """
        # Valid: min_temp <= temp <= max_temp
        record = create_weather_record(
            min_temperature=10.0, temperature=20.0, max_temperature=30.0
        )
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)

        # Invalid: temp < min_temp
        record = create_weather_record(
            min_temperature=20.0, temperature=10.0, max_temperature=30.0
        )
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)

        # Invalid: temp > max_temp
        record = create_weather_record(
            min_temperature=10.0, temperature=40.0, max_temperature=30.0
        )
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)

        # Invalid: min_temp > max_temp
        record = create_weather_record(
            min_temperature=30.0, temperature=20.0, max_temperature=10.0
        )
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)

    def test_temperature_consistency_partial_values(self):
        """
        Test temperature consistency validation when only some temperature values are present.
        """
        # Only temp and min_temp: Valid
        record = create_weather_record(min_temperature=10.0, temperature=20.0)
        record.max_temperature = None
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)

        # Only temp and min_temp: Invalid (temp < min_temp)
        record = create_weather_record(min_temperature=20.0, temperature=10.0)
        record.max_temperature = None
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)

        # Only temp and max_temp: Valid
        record = create_weather_record(temperature=20.0, max_temperature=30.0)
        record.min_temperature = None
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)

        # Only temp and max_temp: Invalid (temp > max_temp)
        record = create_weather_record(temperature=30.0, max_temperature=20.0)
        record.min_temperature = None
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)

    def test_min_max_temperature_consistency(self):
        """
        Test consistency between min and max temperature values.
        """
        # Valid: min_temp < max_temp (no temp value)
        record = create_weather_record(min_temperature=10.0, max_temperature=30.0)
        record.temperature = None
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)

        # Invalid: min_temp > max_temp (no temp value)
        record = create_weather_record(min_temperature=30.0, max_temperature=10.0)
        record.temperature = None
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)

    def test_wind_speed_consistency(self):
        """
        Test wind speed consistency validation.
        Ensures that wind_speed <= max_wind_speed.
        """
        # Valid: wind_speed < max_wind_speed
        record = create_weather_record(wind_speed=10.0, max_wind_speed=20.0)
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)

        # Invalid: wind_speed > max_wind_speed
        record = create_weather_record(wind_speed=30.0, max_wind_speed=20.0)
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)

        record = create_weather_record(max_wind_speed=20.0)
        record.wind_speed = None
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)

    def test_wind_gust_consistency(self):
        """
        Test wind gust consistency validation.
        Ensures that wind_gust <= max_wind_gust.
        """
        # Valid: wind_gust < max_wind_gust
        record = create_weather_record(wind_gust=15.0, max_wind_gust=25.0)
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)

        # Valid: wind_gust = max_wind_gust
        record = create_weather_record(wind_gust=25.0, max_wind_gust=25.0)
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)

        # Invalid: wind_gust > max_wind_gust
        record = create_weather_record(wind_gust=35.0, max_wind_gust=25.0)
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)

        # Test with one value None
        record = create_weather_record(wind_gust=15.0)
        record.max_wind_gust = None
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)

        record = create_weather_record(max_wind_gust=25.0)
        record.wind_gust = None
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)

    def test_wind_speed_gust_consistency(self):
        """
        Test consistency between wind speed and wind gust.
        Ensures that wind_speed <= wind_gust.
        """
        # Valid: wind_speed < wind_gust
        record = create_weather_record(wind_speed=10.0, wind_gust=15.0)
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)

        # Valid: wind_speed = wind_gust
        record = create_weather_record(wind_speed=15.0, wind_gust=15.0)
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)

        # Invalid: wind_speed > wind_gust
        record = create_weather_record(wind_speed=20.0, wind_gust=15.0)
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)

        # Test with one value None
        record = create_weather_record(wind_speed=10.0)
        record.wind_gust = None
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)

        record = create_weather_record(wind_gust=15.0)
        record.wind_speed = None
        result = self.validator.validate(record)
        self.assertFalse(result.flagged)

    def test_consistency_with_multiple_invalid_relationships(self):
        """
        Test validation when multiple consistency rules are violated.
        """
        record = create_weather_record(
            min_temperature=20.0,
            temperature=15.0,  # Inconsistent with min_temp
            max_temperature=10.0,  # Inconsistent with min_temp and temp
            wind_speed=30.0,
            max_wind_speed=20.0,  # Inconsistent with wind_speed
            wind_gust=25.0,  # Inconsistent with wind_speed
            max_wind_gust=15.0,  # Inconsistent with wind_gust
        )
        result = self.validator.validate(record)
        self.assertTrue(result.flagged)


if __name__ == "__main__":
    unittest.main()
