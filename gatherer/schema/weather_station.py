"""
This module defines the schema for weather data gathering and processing.
WeatherStation: Represents a weather station with attributes such as connection
type, pressure offset, and timezones.
"""

import datetime
import uuid
import zoneinfo
from dataclasses import dataclass, field
from typing import Union


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

    def __eq__(self, other):
        """
        Define equality for WeatherStation objects based on their ID.
        """
        if not isinstance(other, WeatherStation):
            return False
        return self.id == other.id

    def __hash__(self):
        """
        Make WeatherStation hashable based on its ID.
        """
        return hash(self.id)
