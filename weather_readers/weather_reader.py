from abc import ABC, abstractmethod
import datetime
from schema import WeatherRecord, WeatherStation
from typing import Any
from dateutil import parser

class WeatherReader(ABC):
    @abstractmethod
    def fetch_data(self, station: WeatherStation, *args, **kwargs) -> dict:
        """
        Fetch data from the source. Subclasses should implement this to handle their specific data-fetching logic.
        """
        pass

    @abstractmethod
    def parse(self, station: WeatherStation, data: dict) -> WeatherRecord:
        """
        Parse the fetched data into a WeatherRecord. Subclasses should implement this to handle their specific parsing logic.
        """
        pass

    def validate_fields(self, station: WeatherStation) -> None:
        """
        Validate the fields of the WeatherStation based on self.required_fields.
        """
        for field in self.required_fields:
            if getattr(station, field) is None:
                raise ValueError(f"Missing required field: {field}")

    def get_data(self, station: WeatherStation, *args, **kwargs) -> WeatherRecord:
        """
        Template method to fetch and parse data. Subclasses can override the other methods as needed.
        """
        self.validate_fields(station)
        raw_data = self.fetch_data(station, *args, **kwargs)

        if raw_data is None:
            return None

        return self.parse(station, raw_data)
    
# region helpers

    def assert_date_age(self, date: datetime.datetime) -> None:
        if date is None:
            raise ValueError("Date is None")
        
        if date.tzinfo is None:
            raise ValueError("Date has no timezone")
        
        if date.tzinfo != datetime.timezone.utc:
            raise ValueError("Date is not UTC")
        
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if (now_utc - date).total_seconds() > 1800:
            raise ValueError(f"Reading timestamp is too old to be stored as current. Observation time (UTC): {date}, current time (UTC): {now_utc}")
        
    def max_or_none(self, arglist) -> Any:
        return max(arglist) if arglist and len(arglist) > 0 else None

    def min_or_none(self, arglist) -> Any:
        return min(arglist) if arglist and len(arglist) > 0 else None

    def coalesce(self, arglist):
        for arg in arglist:
            if arg is not None:
                return arg
        return None

    def smart_azimuth(self, azimuth) -> float:
        if azimuth is None or azimuth == "-" or azimuth == "N/A":
            return None

        if type(azimuth) is not str:
            if type(azimuth) is int or type(azimuth) is float:
                if azimuth < 0 or azimuth > 360:
                    raise ValueError(f"Invalid azimuth value: {azimuth}")
                return azimuth
            else:
                raise ValueError(f"Invalid azimuth value: {azimuth}")
            
        azimuth = azimuth.strip().lower().replace(" ", "").replace("º", "").replace("o", "w")
        
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
            "nnw": 337.5
        }

        if azimuth in translations.keys():
            return translations[azimuth]
        else:
            try:
                return self.smart_parse_float(azimuth)
            except ValueError:
                raise ValueError(f"Invalid azimuth value: {azimuth}")

    def safe_float(self, value):
        return float(value) if value is not None else None

    def safe_int(self, value):
        return int(value) if value is not None else None

    def smart_parse_datetime(self, date_str: str, timezone: datetime.tzinfo = None) -> datetime.datetime:
        def try_parse_datetime(date_str, date_format):
            try:
                return datetime.datetime.strptime(date_str, date_format).replace(tzinfo=timezone)
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
        
    def smart_parse_date(self, date_str: str, timezone: datetime.tzinfo = None) -> datetime.date:
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
        Handles both comma and dot as decimal separator. Removes any non-numeric character other than the separator. Pray.
        """
        if self.is_na_value(float_str):
            return None

        if not float_str:
            return 0.0
        
        if "," in float_str and "." in float_str:
            raise ValueError("Invalid float format: both comma and dot as separators.")
        
        if "," in float_str:
            float_str = float_str.replace(".", "").replace(",", ".")
        
        float_str = "".join([c for c in float_str if c.isdigit() or c == "." or c == "-"])
        
        float_val = float(float_str)

        return float_val

    def is_na_value(self, value: str) -> bool:
        return value is None or value == "-" or value == "N/A" or value == "NA" or value == "NaN"
    
# endregion