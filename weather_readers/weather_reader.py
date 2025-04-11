"""
This module defines the base class `WeatherReader` for implementing weather data readers.
Subclasses of `WeatherReader` are responsible for fetching and parsing weather data
from various sources into a standardized `WeatherRecord` format.
"""

import datetime
import logging

from abc import ABC, abstractmethod
from typing import Any

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

    def __init__(self):
        self.required_fields = []

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
    def parse(self, station: WeatherStation, data: dict) -> WeatherRecord:
        """
        Parse the fetched data into a WeatherRecord.

        Args:
            station (WeatherStation): The weather station object.
            data (dict): The raw data fetched from the source.

        Returns:
            WeatherRecord: The parsed weather record.
        """

    def validate_fields(self, station: WeatherStation) -> None:
        """
        Validate the fields of the WeatherStation based on self.required_fields.

        Args:
            station (WeatherStation): The weather station object.

        Raises:
            ValueError: If a required field is missing.
        """
        for field in self.required_fields:
            if getattr(station, field) is None:
                raise ValueError(f"Missing required field: {field}")

    def get_data(self, station: WeatherStation, *args, **kwargs) -> WeatherRecord:
        """
        Template method to fetch and parse data.

        Args:
            station (WeatherStation): The weather station object.

        Returns:
            WeatherRecord: The parsed weather record.
        """
        self.validate_fields(station)
        raw_data = self.fetch_data(station, *args, **kwargs)

        if raw_data is None:
            return None

        return self.parse(station, raw_data)

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
        logging.info("Requesting %s", url)
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        return response

    # region helpers

    def assert_date_age(self, date: datetime.datetime) -> None:
        """
        Assert that the given date is recent and in UTC.

        Args:
            date (datetime.datetime): The date to validate.

        Raises:
            ValueError: If the date is None, has no timezone, is not UTC, or is too old.
        """
        if date is None:
            raise ValueError("Date is None")

        if date.tzinfo is None:
            raise ValueError("Date has no timezone")

        if date.tzinfo != datetime.timezone.utc:
            raise ValueError("Date is not UTC")

        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if (now_utc - date).total_seconds() > 1800:
            raise ValueError(
                f"""Reading timestamp is too old to be stored as current.
                Observation time (UTC): {date}, current time (UTC): {now_utc}"""
            )

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

        if not isinstance(azimuth, str):
            if isinstance(azimuth, (int, float)):
                if azimuth < 0 or azimuth > 360:
                    raise ValueError(f"Invalid azimuth value: {azimuth}")
                return azimuth
            raise ValueError(f"Invalid azimuth value: {azimuth}")

        azimuth = (
            azimuth.strip().lower().replace(" ", "").replace("º", "").replace("o", "w")
        )

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

        if azimuth in translations:
            return translations[azimuth]
        try:
            return self.smart_parse_float(azimuth)
        except ValueError as e:
            raise ValueError(f"Invalid azimuth value: {azimuth}") from e

    def safe_float(self, value):
        """
        Safely convert a value to float.

        Args:
            value (Any): The value to convert.

        Returns:
            float: The converted float value or None.
        """
        return float(value) if value is not None else None

    def safe_int(self, value):
        """
        Safely convert a value to int.

        Args:
            value (Any): The value to convert.

        Returns:
            int: The converted int value or None.
        """
        return int(value) if value is not None else None

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

        def try_parse_datetime(date_str, date_format):
            try:
                return datetime.datetime.strptime(date_str, date_format).replace(
                    tzinfo=timezone
                )
            except ValueError:
                return None

        def get_closest_datetime(dates, now):
            valid_dates = [date for date in dates if date is not None and date <= now]
            if not valid_dates:
                return None
            return min(valid_dates, key=lambda date: abs((date - now).days))

        # Try Spanish formatting
        spanish_formats = ["%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M", "%d/%m/%y %H:%M"]
        spanish_dates = [try_parse_datetime(date_str, fmt) for fmt in spanish_formats]
        spanish = next((date for date in spanish_dates if date is not None), None)

        # Try American formatting
        try:
            american = parser.parse(date_str).replace(tzinfo=timezone)
        except ValueError:
            american = None

        if spanish is None and american is None:
            raise ValueError(f"Invalid date format: {date_str}")

        now = datetime.datetime.now(tz=timezone)
        if spanish is not None and american is not None:
            return get_closest_datetime([spanish, american], now)

        return spanish if spanish is not None else american

    def smart_parse_date(
        self, date_str: str, timezone: datetime.tzinfo = None
    ) -> datetime.date:
        """
        Parse a date string into a date object, trying multiple formats.

        Args:
            date_str (str): The date string to parse.
            timezone (datetime.tzinfo, optional): The timezone to apply.

        Returns:
            datetime.date: The parsed date object.

        Raises:
            ValueError: If the date string is invalid.
        """

        def try_parse_date(date_str, date_format):
            try:
                return datetime.datetime.strptime(date_str, date_format).date()
            except ValueError:
                return None

        def get_closest_date(dates, now):
            valid_dates = [date for date in dates if date is not None and date <= now]
            if not valid_dates:
                return None
            return min(valid_dates, key=lambda date: abs((date - now).days))

        # Try Spanish formatting
        spanish_formats = ["%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%y"]
        spanish_dates = [try_parse_date(date_str, fmt) for fmt in spanish_formats]
        spanish = next((date for date in spanish_dates if date is not None), None)

        # Try American formatting
        try:
            american = parser.parse(date_str).date()
        except ValueError:
            american = None

        if spanish is None and american is None:
            raise ValueError(f"Invalid date format: {date_str}")

        now = datetime.datetime.now(tz=timezone).date()
        if spanish is not None and american is not None:
            return get_closest_date([spanish, american], now)

        return spanish if spanish is not None else american

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
