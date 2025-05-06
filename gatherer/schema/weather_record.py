"""
This module defines the schema for weather data gathering and processing.
WeatherRecord: Represents a single weather data record with attributes such
as temperature, wind speed, humidity, etc.
"""

from dataclasses import dataclass
import datetime
import uuid
from typing import Optional


@dataclass
class WeatherRecord:
    """
    A class to represent a weather record with various meteorological parameters.

    Attributes:
        id (uuid.UUID): Unique identifier for the weather record.
        station_id (uuid.UUID): Unique identifier for the weather station.
        source_timestamp (datetime.datetime): Timestamp of the recorded data.
        temperature (float): Current temperature in degrees Celsius.
        wind_speed (float): Current wind speed in meters per second.
        max_wind_speed (float): Maximum wind speed recorded for the day in meters per second.
        wind_direction (float): Wind direction in degrees (0-360).
        rain (float): Rainfall amount in millimeters.
        cumulative_rain (float): Cumulative rainfall amount in millimeters.
        humidity (float): Humidity percentage (0-100).
        pressure (float): Atmospheric pressure in hPa.
        flagged (bool): Indicates if the record contains invalid or flagged data.
        gatherer_thread_id (uuid.UUID): Identifier for the gatherer thread that collected the data.
        max_temperature (float): Maximum temperature recorded for the day in degrees Celsius.
        min_temperature (float): Minimum temperature recorded for the day in degrees Celsius.
        wind_gust (float): Current wind gust speed in meters per second.
        max_wind_gust (float): Maximum wind gust speed recorded for the day in meters per second.
        taken_timestamp (datetime.datetime): Timestamp when the record was created.
    """

    id: uuid.UUID
    station_id: uuid.UUID
    source_timestamp: datetime.datetime
    taken_timestamp: datetime.datetime
    gatherer_thread_id: uuid.UUID
    temperature: Optional[float] = None
    wind_speed: Optional[float] = None
    max_wind_speed: Optional[float] = None
    wind_direction: Optional[float] = None
    rain: Optional[float] = None
    cumulative_rain: Optional[float] = None
    humidity: Optional[float] = None
    pressure: Optional[float] = None
    flagged: bool = False
    max_temperature: Optional[float] = None
    min_temperature: Optional[float] = None
    wind_gust: Optional[float] = None
    max_wind_gust: Optional[float] = None
