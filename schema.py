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

import datetime
import uuid
import zoneinfo
from dataclasses import dataclass, field
from typing import Dict, Optional, Union


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


@dataclass
class GathererThread:
    """
    A class to represent a thread responsible for gathering weather data.

    Attributes:
        id (uuid.UUID): Unique identifier for the gatherer thread.
        thread_timestamp (datetime.datetime): Timestamp of the thread's execution.
        total_stations (int): Total number of weather stations being monitored.
        error_stations (int): Number of weather stations with errors.
        errors (dict): Dictionary containing error messages and their corresponding station IDs.
        command (str): Command that executed the thread.
    """

    id: uuid.UUID
    thread_timestamp: datetime.datetime
    total_stations: int
    error_stations: int
    errors: Dict
    command: str


@dataclass
class WeatherStation:
    """
    A class to represent a weather station with various connection parameters.

    Attributes:
        id (uuid.UUID): Unique identifier for the weather station.
        connection_type (str): Type of connection used by the weather station.
        field1 (str): First field used for connection (e.g., station ID).
        field2 (str): Second field used for connection (e.g., password).
        field3 (str): Third field used for connection (e.g., API key).
        pressure_offset (float): Offset value to be applied to atmospheric pressure.
        data_timezone (zoneinfo.ZoneInfo): Timezone information for data collection.
        local_timezone (zoneinfo.ZoneInfo): Local timezone information for the station.
    """

    id: uuid.UUID
    connection_type: str
    field1: str
    field2: str
    field3: str
    pressure_offset: float
    _data_timezone: Union[str, datetime.tzinfo]
    _local_timezone: Union[str, datetime.tzinfo]
    data_timezone: zoneinfo.ZoneInfo = field(init=False)
    local_timezone: zoneinfo.ZoneInfo = field(init=False)

    def __post_init__(self):
        """
        Post-initialization processing to convert timezone strings to ZoneInfo objects.
        """
        # Handle data_timezone
        if isinstance(self._data_timezone, str):
            self.data_timezone = zoneinfo.ZoneInfo(self._data_timezone)
        elif isinstance(self._data_timezone, datetime.tzinfo):
            self.data_timezone = self._data_timezone
        else:
            raise ValueError("data_timezone must be a string or a tzinfo object")

        # Handle local_timezone
        if isinstance(self._local_timezone, str):
            self.local_timezone = zoneinfo.ZoneInfo(self._local_timezone)
        elif isinstance(self._local_timezone, datetime.tzinfo):
            self.local_timezone = self._local_timezone
        else:
            raise ValueError("local_timezone must be a string or a tzinfo object")
