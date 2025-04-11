"""
This module provides utility functions for unit conversion.

The `UnitConverter` class includes methods to convert between various units
commonly used in weather data, such as temperature, pressure, speed, and precipitation.
"""


class UnitConverter:
    """
    Stores helper logic for common unit conversion calculations.
    """

    @classmethod
    def fahrenheit_to_celsius(cls, fahrenheit: float) -> float:
        """
        Convert temperature from Fahrenheit to Celsius.

        Args:
            fahrenheit (float): Temperature in Fahrenheit.

        Returns:
            float: Temperature in Celsius.
        """
        return round((fahrenheit - 32) * 5 / 9, 4) if fahrenheit is not None else None

    @classmethod
    def psi_to_hpa(cls, pressure: float) -> float:
        """
        Convert pressure from PSI to hPa.

        Args:
            pressure (float): Pressure in PSI.

        Returns:
            float: Pressure in hPa.
        """
        return round(pressure * 33.8639, 4) if pressure is not None else None

    @classmethod
    def mph_to_kph(cls, speed: float) -> float:
        """
        Convert speed from MPH to KPH.

        Args:
            speed (float): Speed in MPH.

        Returns:
            float: Speed in KPH.
        """
        return round(speed * 1.60934, 4) if speed is not None else None

    @classmethod
    def inches_to_mm(cls, inches: float) -> float:
        """
        Convert precipitation from inches to millimeters.

        Args:
            inches (float): Precipitation in inches.

        Returns:
            float: Precipitation in millimeters.
        """
        return round(inches * 25.4, 4) if inches is not None else None
