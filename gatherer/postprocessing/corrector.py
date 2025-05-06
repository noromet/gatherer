"""
This module contains the Corrector class, which is responsible for
correcting weather data records. It provides methods to apply
pressure offsets and rounding corrections to the weather data.
"""

from typing import Optional

from gatherer.schema import WeatherRecord


class Corrector:
    """
    A class to correct the data in the WeatherRecord.
    """

    def apply_pressure_offset(
        self, record: WeatherRecord, offset: Optional[float]
    ) -> WeatherRecord:
        """
        Applies an offset to the atmospheric pressure.

        Args:
            pressure: The pressure value.
            offset: The offset value to be added to the pressure.

        Returns:
            Adjusted pressure value or None if input is None.
        """
        if record.pressure is not None and offset is not None:
            record.pressure += offset

        return record

    def apply_rounding(self, record: WeatherRecord, decimals: int = 1) -> WeatherRecord:
        """
        Rounds numerical weather attributes to the specified number of decimal places.

        Args:
            data_dict: Dictionary containing weather data attributes.
            decimals: The number of decimal places to round to.

        Returns:
            Dictionary with rounded values.
        """

        numerical_fields = [
            "temperature",
            "wind_speed",
            "max_wind_speed",
            "humidity",
            "pressure",
            "rain",
            "cumulative_rain",
            "max_temperature",
            "min_temperature",
            "wind_gust",
            "max_wind_gust",
        ]

        for field in numerical_fields:
            value = getattr(record, field)
            if value is not None:
                rounded_value = round(value, decimals)
                setattr(record, field, rounded_value)

        return record

    def correct(
        self, record: WeatherRecord, offset: float = 0, decimals: int = 1
    ) -> WeatherRecord:
        """
        Applies pressure offset and rounding corrections to the WeatherRecord.

        Args:
            record: The WeatherRecord to correct.
            offset: The pressure offset to apply.
            decimals: The number of decimal places to round to.

        Returns:
            A new WeatherRecord with corrections applied.
        """
        record = self.apply_pressure_offset(record, offset)
        record = self.apply_rounding(record, decimals)

        return record
