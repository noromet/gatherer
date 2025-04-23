"""
This module provides the `Validator` class for validating weather data
against predefined safe ranges.
"""

from typing import Any, Dict, Tuple
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

    def validate(self, record: WeatherRecord) -> Tuple[Dict[str, Any], bool]:
        """
        Validates weather data against predefined safe ranges and flags invalid data.

        Args:
            data_dict: Dictionary containing weather data attributes.

        Returns:
            Tuple of (validated data dictionary, flagged status)
        """
        for attribute, safe_range in WEATHER_SAFE_RANGES.items():
            value = getattr(record, attribute)

            if value is not None and not safe_range[0] <= value <= safe_range[1]:
                # If the value is outside the safe range, flag the record and set the value to None
                record.flagged = True
                setattr(record, attribute, None)

        return record
