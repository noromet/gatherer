"""
This module provides the `Validator` class for validating weather data
against predefined safe ranges.
"""

from typing import Tuple
from schema import WeatherRecord


WEATHER_SAFE_RANGES = {
    "temperature": (-39, 50),
    "wind_speed": (0, 500),
    "humidity": (0, 100),
    "pressure": (800, 1100),
    "wind_direction": (0, 360),
    "rain": (0, 500),
    "cumulative_rain": (0, 15000),
    "max_temperature": (-39, 50),
    "min_temperature": (-39, 50),
    "max_wind_speed": (0, 500),
    "wind_gust": (0, 500),
    "max_wind_gust": (0, 500),
}


class Validator:
    """
    A class to validate weather data against predefined safe ranges.
    """

    def validate(self, record: WeatherRecord) -> WeatherRecord:
        """
        Validates weather data against predefined safe ranges and flags invalid data.
        Also checks for consistency between related fields.

        Args:
            record: WeatherRecord containing weather data attributes.

        Returns:
            Validated WeatherRecord with flagged status and cleared invalid values.
        """
        # First validate against safe ranges
        self._validate_safe_ranges(record)

        # Then validate consistency between related fields
        self._validate_consistency(record)

        return record

    def _validate_safe_ranges(self, record: WeatherRecord) -> None:
        """
        Validates that each weather attribute falls within its defined safe range.

        Args:
            record: WeatherRecord to validate
        """
        for attribute, safe_range in WEATHER_SAFE_RANGES.items():
            value = getattr(record, attribute)

            if value is not None and not safe_range[0] <= value <= safe_range[1]:
                # If the value is outside the safe range, flag the record and set the value to None
                record.flagged = True
                setattr(record, attribute, None)

    def _validate_consistency(self, record: WeatherRecord) -> None:
        """
        Validates consistency between related fields.
        Checks for:
        - min_temperature <= temperature <= max_temperature
        - wind_speed <= max_wind_speed
        - wind_gust <= max_wind_gust
        - wind_speed <= wind_gust

        Args:
            record: WeatherRecord to validate
        """
        # Validate temperature relationships with a helper function
        self._validate_pair_relationship(
            record,
            [
                ("min_temperature", "temperature"),
                ("temperature", "max_temperature"),
                ("min_temperature", "max_temperature"),
            ],
            lambda a, b: a <= b,
        )

        # Validate wind speed relationships
        self._validate_pair_relationship(
            record,
            [
                ("wind_speed", "max_wind_speed"),
                ("wind_gust", "max_wind_gust"),
                ("wind_speed", "wind_gust"),
            ],
            lambda a, b: a <= b,
        )

    def _validate_pair_relationship(
        self, record: WeatherRecord, field_pairs: list[Tuple[str, str]], comparison_func
    ) -> None:
        """
        Helper method to validate relationships between pairs of fields.

        Args:
            record: WeatherRecord to validate
            field_pairs: List of field name tuples to compare
            comparison_func: Function that defines the expected relationship between values
        """
        for field_a, field_b in field_pairs:
            value_a = getattr(record, field_a)
            value_b = getattr(record, field_b)

            if value_a is not None and value_b is not None:
                if not comparison_func(value_a, value_b):
                    record.flagged = True
