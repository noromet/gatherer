import datetime
from dateutil import parser
from typing import Any

def smart_azimuth(azimuth) -> float:
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

def smart_parse_date(date_str: str) -> datetime.datetime:
    #try spanish formatting
    spanish = None
    try:
        spanish_long_year_format_date = datetime.datetime.strptime(date_str, "%d/%m/%Y %H:%M")
        spanish = spanish_long_year_format_date
    except ValueError:
        pass

    if spanish is None:
        try:
            spanish_short_year_format_date = datetime.datetime.strptime(date_str, "%d/%m/%y %H:%M")
            spanish = spanish_short_year_format_date
        except ValueError:
            pass

    #try american formatting
    american = None
    try:
        american_format_date = parser.parse(date_str)
        american = american_format_date
    except ValueError as e:
        pass

    if spanish is None and american is None:
        raise ValueError(f"Invalid date format: {date_str}")
    
    if spanish is not None and american is not None:
        if abs((spanish - datetime.datetime.now()).days) < abs((american - datetime.datetime.now()).days):
            return spanish
        else:
            return american
        
    if spanish is not None:
        return spanish
    
    if american is not None:
        return american
    
    raise ValueError(f"Invalid date format: {date_str}")
    
def smart_parse_float(float_str: str) -> float:
    """
    Handles both comma and dot as decimal separator. Removes any non-numeric character other than the separator. Pray.
    """

    if not float_str:
        return 0.0
    
    if "," in float_str and "." in float_str:
        raise ValueError("Invalid float format: both comma and dot as separators.")
    
    if "," in float_str:
        float_str = float_str.replace(".", "").replace(",", ".")
    
    float_str = "".join([c for c in float_str if c.isdigit() or c == "."])
    
    return float(float_str)

def is_date_too_old(date: datetime.datetime) -> bool: #1hr
    now_minus_1_hour = (datetime.datetime.now(date.tzinfo) - datetime.timedelta(hours=1))
    return date < now_minus_1_hour