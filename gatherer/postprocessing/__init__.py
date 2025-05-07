"""
This package provides postprocessing functionality for weather data.

The exported classes and functions include:
- Validator: A class to validate weather data against predefined safe ranges.
- Corrector: A class to correct the data in the WeatherRecord.
"""

from .validator import Validator
from .corrector import Corrector

__all__ = ["Validator", "Corrector"]
