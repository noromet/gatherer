"""
This module defines the schema for weather data gathering and processing.

Classes:
- WeatherRecord: Represents a single weather data record with attributes
such as temperature, wind speed, humidity, etc.
  Includes methods for data validation, applying offsets, and rounding values.
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

    Methods:
        sanity_check():
            Validates the weather data against predefined safe ranges and flags invalid data.
        apply_pressure_offset(offset: float):
            Applies an offset to the atmospheric pressure.
        apply_rounding(decimals: int = 1):
            Rounds numerical attributes to the specified number of decimal places.
    """

    def __init__(
        self,
        wr_id: uuid.uuid4,
        station_id: uuid.uuid4,
        source_timestamp: datetime.datetime,
        temperature: float,
        wind_speed: float,
        max_wind_speed: float,
        wind_direction: float,
        rain: float,
        humidity: float,
        pressure: float,
        flagged: bool,
        gatherer_thread_id: uuid.uuid4,
        cumulative_rain: float,
        max_temperature: float,
        min_temperature: float,
        wind_gust: float,
        max_wind_gust: float,
    ):

        self.id = wr_id
        self.station_id = station_id
        self.source_timestamp = source_timestamp
        self.temperature = temperature
        self.wind_speed = wind_speed
        self.max_wind_speed = max_wind_speed  # today
        self.wind_direction = wind_direction
        self.rain = rain
        self.cumulative_rain = cumulative_rain
        self.humidity = humidity
        self.pressure = pressure
        self.flagged = flagged
        self.taken_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        self.gatherer_thread_id = gatherer_thread_id
        self.max_temperature = max_temperature  # today
        self.min_temperature = min_temperature  # today
        self.wind_gust = wind_gust
        self.max_wind_gust = max_wind_gust

    def sanity_check(self):
        """
        Validates the weather data against predefined safe ranges and flags invalid data.
        Sets the flagged attribute to True if any data point is outside the safe range.
        """

        def validate_range(attribute, value, safe_range):
            if value is not None and not safe_range[0] < value < safe_range[1]:
                setattr(self, attribute, None)
                self.flagged = True

        ranges = {
            "temperature": (-39, 50),
            "max_temperature": (-39, 50),
            "min_temperature": (-39, 50),
            "wind_speed": (0, 500),
            "max_wind_speed": (0, 500),
            "wind_gust": (0, 500),
            "max_wind_gust": (0, 500),
            "humidity": (0, 100),
            "pressure": (800, 1100),
        }

        for attr, safe_range in ranges.items():
            validate_range(attr, getattr(self, attr, None), safe_range)

        if self.wind_direction is not None and not 0 <= self.wind_direction <= 360:
            self.wind_direction = None
            self.flagged = True

    def apply_pressure_offset(self, offset: float):
        """
        Applies an offset to the atmospheric pressure.
        Args:
            offset (float): The offset value to be added to the pressure.
        """
        if offset and self.pressure is not None:
            self.pressure += offset

    def apply_rounding(self, decimals: int = 1):
        """
        Rounds numerical attributes to the specified number of decimal places.
        Args:
            decimals (int): The number of decimal places to round to.
        """
        if self.temperature is not None:
            self.temperature = round(self.temperature, decimals)
        if self.wind_speed is not None:
            self.wind_speed = round(self.wind_speed, decimals)
        if self.max_wind_speed is not None:
            self.max_wind_speed = round(self.max_wind_speed, decimals)
        if self.humidity is not None:
            self.humidity = round(self.humidity, decimals)
        if self.pressure is not None:
            self.pressure = round(self.pressure, decimals)
        if self.rain is not None:
            self.rain = round(self.rain, decimals)
        if self.cumulative_rain is not None:
            self.cumulative_rain = round(self.cumulative_rain, decimals)
        if self.max_temperature is not None:
            self.max_temperature = round(self.max_temperature, decimals)
        if self.min_temperature is not None:
            self.min_temperature = round(self.min_temperature, decimals)
        if self.wind_gust is not None:
            self.wind_gust = round(self.wind_gust, decimals)
        if self.max_wind_gust is not None:
            self.max_wind_gust = round(self.max_wind_gust, decimals)


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

    def __init__(
        self,
        gt_id: uuid.uuid4,
        thread_timestamp: datetime.datetime,
        total_stations: int,
        error_stations: int,
        errors: dict,
        command: str,
    ):
        self.id = gt_id
        self.thread_timestamp = thread_timestamp
        self.total_stations = total_stations
        self.error_stations = error_stations
        self.errors = errors
        self.command = command


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

    def __init__(
        self,
        ws_id: uuid.uuid4,
        connection_type: str,
        field1: str,
        field2: str,
        field3: str,
        pressure_offset: float,
        data_timezone: datetime.tzinfo,
        local_timezone: datetime.tzinfo,
    ):
        self.id = ws_id
        self.connection_type = connection_type
        self.field1 = field1
        self.field2 = field2
        self.field3 = field3
        self.pressure_offset = pressure_offset
        self.data_timezone = zoneinfo.ZoneInfo(data_timezone)
        self.local_timezone = zoneinfo.ZoneInfo(local_timezone)
