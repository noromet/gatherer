"""
This module defines the base class `WeatherReader` for implementing weather data readers.
Subclasses of `WeatherReader` are responsible for fetching and parsing weather data
from various sources into a standardized `WeatherRecord` format.
"""

import datetime
import logging
from abc import ABC, abstractmethod
from typing import Any
import uuid

import requests
from dateutil import parser

from schema import WeatherRecord, WeatherStation


class WeatherReader(ABC):
    """
    Abstract base class for weather data readers.

    This class provides a template for fetching and parsing weather data from different sources.
    Subclasses must implement the `fetch_data` and `parse` methods to handle source-specific logic.
    It also includes utility methods for data validation and transformation.
    """

    def __init__(self, ignore_early_readings: bool = False):
        self.required_fields = []
        self.ignore_early_readings = ignore_early_readings
        self.max_reading_age_seconds = 1800  # 30 minutes

    # region template methods
    @abstractmethod
    def fetch_data(self, station: WeatherStation) -> dict:
        """
        Fetch data from the source.

        Args:
            station (WeatherStation): The weather station object.

        Returns:
            dict: The raw data fetched from the source.
        """

    @abstractmethod
    def parse(self, station: WeatherStation, data: dict) -> dict:
        """
        Parse the fetched data into a WeatherRecord.

        Args:
            station (WeatherStation): The weather station object.
            data (dict): The raw data fetched from the source.

        Returns:
            dict: The fields dict with all the data from the source.
        """

    # endregion

    # region common methods
    def validate_connection_fields(self, station: WeatherStation) -> bool:
        """
        Validate the fields of the WeatherStation based on self.required_fields.

        Args:
            station (WeatherStation): The weather station object.

        Raises:
            ValueError: If a required field is missing.
        """
        for field in self.required_fields:
            if getattr(station, field) is None:
                return False

        return True

    def get_data(self, station: WeatherStation, *args, **kwargs) -> WeatherRecord:
        """
        Template method to fetch and parse data.

        Args:
            station (WeatherStation): The weather station object.

        Returns:
            WeatherRecord: The parsed weather record.
        """
        if not self.validate_connection_fields(station):
            raise ValueError(
                f"Station {station.id} is missing a required connection field."
            )

        raw_data = self.fetch_data(station, *args, **kwargs)

        if raw_data is None:
            return None

        fields = self.parse(station, raw_data)

        if fields is None:
            return None

        is_valid, error_message = self.validate_date_age(fields["source_timestamp"])
        if not is_valid:
            logging.error(
                "Invalid date for station %s: %s",
                station.id,
                error_message,
            )

        use_daily = True

        if self.ignore_early_readings:
            if (
                fields["source_timestamp"].hour == 0
                and fields["source_timestamp"].minute < 60
            ):
                # Check if the source timestamp is from 00:00 AM to 01:00 AM
                if fields["source_timestamp"].minute < 60:
                    use_daily = False
                    logging.warning(
                        "Ignoring early reading from %s: %s",
                        station.id,
                        fields["source_timestamp"],
                    )
                else:
                    use_daily = True

        return self.build_weather_record(fields, station, use_daily)

    def get_fields(self) -> dict:
        """
        Return an empty fields dictionary to be populated by the subclass.
        Two values have defaults:
            - flagged: False
            - taken_timestamp: datetime.datetime.now(tz=datetime.timezone.utc)
        """
        return {
            "source_timestamp": None,
            "taken_timestamp": datetime.datetime.now(tz=datetime.timezone.utc),
            "instant": {
                "temperature": None,
                "wind_speed": None,
                "wind_direction": None,
                "rain": None,
                "humidity": None,
                "pressure": None,
                "wind_gust": None,
            },
            "daily": {
                "max_temperature": None,
                "min_temperature": None,
                "max_wind_speed": None,
                "max_wind_gust": None,
                "cumulative_rain": None,
            },
            "flagged": False,
        }

    def build_weather_record(
        self, fields: dict, station: WeatherStation, use_daily=True
    ) -> WeatherRecord:
        """
        Build a WeatherRecord object from the provided fields and station.

        Args:
            fields (dict): The fields to populate in the WeatherRecord.
            station (WeatherStation): The weather station object.

        Returns:
            WeatherRecord: The constructed WeatherRecord object.
        """
        return WeatherRecord(
            wr_id=str(uuid.uuid4()),
            station_id=station.id,
            source_timestamp=fields.get("source_timestamp"),
            taken_timestamp=fields.get("taken_timestamp"),
            flagged=fields.get("flagged"),
            gatherer_thread_id=None,
            temperature=fields.get("instant").get("temperature"),
            wind_speed=fields.get("instant").get("wind_speed"),
            wind_direction=fields.get("instant").get("wind_direction"),
            rain=fields.get("instant").get("rain"),
            humidity=fields.get("instant").get("humidity"),
            pressure=fields.get("instant").get("pressure"),
            wind_gust=fields.get("instant").get("wind_gust"),
            max_temperature=(
                fields.get("daily").get("max_temperature") if use_daily else None
            ),
            min_temperature=(
                fields.get("daily").get("min_temperature") if use_daily else None
            ),
            cumulative_rain=(
                fields.get("daily").get("cumulative_rain") if use_daily else None
            ),
            max_wind_speed=(
                fields.get("daily").get("max_wind_speed") if use_daily else None
            ),
            max_wind_gust=(
                fields.get("daily").get("max_wind_gust") if use_daily else None
            ),
        )

    # endregion

    # region helper methods

    def make_request(
        self, url: str, params: dict = None, headers: dict = None, timeout: int = 5
    ) -> requests.Response:
        """
        Helper function to make a GET request with a default
        timeout and optional headers and parameters.

        Args:
            url (str): The URL to request.
            params (dict, optional): Query parameters for the request.
            headers (dict, optional): Headers for the request.
            timeout (int, optional): Timeout for the request in seconds.

        Returns:
            requests.Response: The response object.
        """
        if headers is None:
            headers = {}
        if params is None:
            params = {}

        response = requests.get(url, params=params, headers=headers, timeout=timeout)

        if response.status_code not in [200, 201, 204]:
            logging.error("Failed to fetch data from %s: %s", url, response.status_code)
            return None

        logging.info("Requesting %s", response.url)

        return response

    def validate_date_age(self, date: datetime.datetime) -> tuple[bool, str]:
        """
        Assert that the given date is recent, has a timezone, and is not in the future.

        Args:
            date (datetime.datetime): The date to validate.

        Raises:
            ValueError: If the date is None, has no timezone, is too old, or is in the future.
        """
        if date is None:
            return False, "Date is None"

        if date.tzinfo is None:
            return False, "Date has no timezone"

        # Normalize the date to UTC
        date_utc = date.astimezone(datetime.timezone.utc)

        now_utc = datetime.datetime.now(datetime.timezone.utc)

        if date_utc > now_utc:
            return (
                False,
                f"Date is in the future. Observation time (UTC): {date_utc}, "
                f"current time (UTC): {now_utc}",
            )

        if (now_utc - date_utc).total_seconds() > self.max_reading_age_seconds:
            return (
                False,
                f"Reading timestamp is too old. Observation time (UTC): {date_utc}, "
                f"current time (UTC): {now_utc}",
            )

        return True, None

    def max_or_none(self, arglist) -> Any:
        """
        Return the maximum value in the list or None if the list is empty.

        Args:
            arglist (list): The list of values.

        Returns:
            Any: The maximum value or None.
        """
        return max(arglist) if arglist and len(arglist) > 0 else None

    def min_or_none(self, arglist) -> Any:
        """
        Return the minimum value in the list or None if the list is empty.

        Args:
            arglist (list): The list of values.

        Returns:
            Any: The minimum value or None.
        """
        return min(arglist) if arglist and len(arglist) > 0 else None

    def coalesce(self, arglist):
        """
        Return the first non-None value in the list.

        Args:
            arglist (list): The list of values.

        Returns:
            Any: The first non-None value or None.
        """
        if not arglist:
            return None

        for arg in arglist:
            if arg is not None:
                return arg
        return None

    def smart_azimuth(self, azimuth) -> float:
        """
        Convert azimuth value to a float representing degrees.

        Args:
            azimuth (Any): The azimuth value.

        Returns:
            float: The azimuth in degrees.

        Raises:
            ValueError: If the azimuth value is invalid.
        """
        if azimuth is None or azimuth == "-" or azimuth == "N/A":
            return None

        def _azimuth_as_float(azimuth: int | float):
            if azimuth < 0 or azimuth > 360:
                return None
            if azimuth == 360:
                return 0
            return azimuth

        def _azimuth_as_string(azimuth: str):
            azimuth = azimuth.strip().lower().replace("o", "w")

            clean_azimuth = ""
            for character in azimuth:
                if character in "0123456789nesw":
                    clean_azimuth += character

            translations = {
                "n": 0,
                "nne": 22.5,
                "ne": 45,
                "ene": 67.5,
                "e": 90,
                "ese": 112.5,
                "se": 135,
                "sse": 157.5,
                "s": 180,
                "ssw": 202.5,
                "sw": 225,
                "wsw": 247.5,
                "w": 270,
                "wnw": 292.5,
                "nw": 315,
                "nnw": 337.5,
            }

            if clean_azimuth in translations:
                return translations[clean_azimuth]

            try:
                parsed_float = self.smart_parse_float(clean_azimuth)
                if parsed_float is None or parsed_float < 0 or parsed_float > 360:
                    return None
                return parsed_float
            except ValueError:
                return None

        if isinstance(azimuth, (int, float)):
            return _azimuth_as_float(azimuth)
        if isinstance(azimuth, (str)):
            return _azimuth_as_string(azimuth)
        return None

    def safe_float(self, value):
        """
        Safely convert a value to float.

        Args:
            value (Any): The value to convert.

        Returns:
            float: The converted float value or None.
        """
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None

    def safe_int(self, value):
        """
        Safely convert a value to int.

        Args:
            value (Any): The value to convert.

        Returns:
            int: The converted int value or None.
        """
        try:
            return int(value) if value is not None else None
        except (ValueError, TypeError):
            return None

    def smart_parse_datetime(
        self, date_str: str, timezone: datetime.tzinfo = None
    ) -> datetime.datetime:
        """
        Parse a date string into a datetime object, trying multiple formats.

        Args:
            date_str (str): The date string to parse.
            timezone (datetime.tzinfo, optional): The timezone to apply.

        Returns:
            datetime.datetime: The parsed datetime object.

        Raises:
            ValueError: If the date string is invalid.
        """
        if date_str.count(":") < 1:
            raise ValueError(f"Parsed date lacks hour and minute: {date_str}")

        def try_parse_datetime(date_str, date_format):
            try:
                return datetime.datetime.strptime(date_str, date_format).replace(
                    tzinfo=timezone
                )
            except ValueError:
                return None

        def get_closest_datetime(dates):
            now = datetime.datetime.now(tz=timezone)
            valid_dates = [date for date in dates if date is not None and date <= now]
            if not valid_dates:
                return None
            return min(valid_dates, key=lambda date: abs((date - now).days))

        # Try Spanish formatting
        spanish_formats = ["%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M", "%d/%m/%y %H:%M"]
        spanish_dates = [try_parse_datetime(date_str, fmt) for fmt in spanish_formats]
        spanish = next((date for date in spanish_dates if date is not None), None)

        # Try auto formatting
        try:
            auto = parser.parse(date_str).replace(tzinfo=timezone)
        except ValueError:
            auto = None

        if spanish is None and auto is None:
            raise ValueError(f"Invalid date format: {date_str}")

        if spanish is not None:
            if auto is not None:
                date = get_closest_datetime([spanish, auto])
            else:
                date = spanish
        else:
            date = auto

        if date is None:
            return None

        if date > datetime.datetime.now(tz=timezone):
            return None

        return date

    def smart_parse_float(self, float_str: str) -> float:
        """
        Parse a string into a float, handling various formats.

        Args:
            float_str (str): The string to parse.

        Returns:
            float: The parsed float value.

        Raises:
            ValueError: If the string format is invalid.
        """
        if self.is_na_value(float_str):
            return None

        if not float_str:
            return 0.0

        if "," in float_str and "." in float_str:
            raise ValueError("Invalid float format: both comma and dot as separators.")

        if "," in float_str:
            float_str = float_str.replace(".", "").replace(",", ".")

        float_str = "".join(
            [c for c in float_str if c.isdigit() or c == "." or c == "-"]
        )

        float_val = float(float_str)

        return float_val

    def is_na_value(self, value: str) -> bool:
        """
        Check if a value represents a "not available" value.

        Args:
            value (str): The value to check.

        Returns:
            bool: True if the value is "not available", False otherwise.
        """
        return (
            value is None
            or value == "-"
            or value == "N/A"
            or value == "NA"
            or value == "NaN"
        )


# endregion
