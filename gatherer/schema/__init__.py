"""
This module defines the schema for weather data gathering and processing.

Classes:
- WeatherRecord: Represents a single weather data record with attributes
such as temperature, wind speed, humidity, etc.
- GathererThread: Represents a thread responsible for gathering weather data,
 including metadata about the process.
- WeatherStation: Represents a weather station with attributes such as
 connection type, pressure offset, and timezones.

The module is designed to facilitate the collection, validation, and
 processing of weather data from various sources.
"""

from .gatherer_thread import GathererThread
from .weather_record import WeatherRecord
from .weather_station import WeatherStation

__all__ = ["GathererThread", "WeatherRecord", "WeatherStation"]
