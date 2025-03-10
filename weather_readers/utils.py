import datetime
from dateutil import parser
from typing import Any

def max_or_none(arglist) -> Any:
    return max(arglist) if arglist and len(arglist) > 0 else None

def min_or_none(arglist) -> Any:
    return min(arglist) if arglist and len(arglist) > 0 else None

def coalesce(arglist):
    for arg in arglist:
        if arg is not None:
            return arg
    return None

def smart_azimuth(azimuth) -> float:
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
            return smart_parse_float(azimuth)
        except ValueError:
            raise ValueError(f"Invalid azimuth value: {azimuth}")

def safe_float(value):
    return float(value) if value is not None else None

def safe_int(value):
    return int(value) if value is not None else None

def smart_parse_datetime(date_str: str, timezone: datetime.tzinfo = None) -> datetime.datetime:
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
    
def smart_parse_date(date_str: str, timezone: datetime.tzinfo = None) -> datetime.date:
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

def smart_parse_float(float_str: str) -> float:
    """
    Handles both comma and dot as decimal separator. Removes any non-numeric character other than the separator. Pray.
    """
    if is_na_value(float_str):
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

def is_na_value(value: str) -> bool:
    return value is None or value == "-" or value == "N/A" or value == "NA" or value == "NaN"

class UnitConverter:
    @staticmethod
    def fahrenheit_to_celsius(fahrenheit: float) -> float:
        return round((fahrenheit - 32) * 5/9, 4) if fahrenheit is not None else None

    @staticmethod
    def psi_to_hpa(pressure: float) -> float:
        return round(pressure * 33.8639, 4) if pressure is not None else None
    
    @staticmethod
    def mph_to_kph(speed: float) -> float:
        return round(speed * 1.60934, 4) if speed is not None else None

    @staticmethod
    def inches_to_mm(inches: float) -> float:
        return round(inches * 25.4, 4) if inches is not None else None