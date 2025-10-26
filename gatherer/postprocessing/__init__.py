"""
This package provides postprocessing functionality for weather data.

The exported classes and functions include:
- Validator: A class to validate weather data against predefined safe ranges.
- Corrector: A class to correct the data in the WeatherRecord.
"""

from .corrector import Corrector
from .validator import Validator

__all__ = ["Validator", "Corrector"]
