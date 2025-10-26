"""
This package holds the WeatherReader classes for various weather data sources.
"""

from .ecowitt_reader import EcowittReader
from .govee_reader import GoveeReader
from .holfuy_reader import HolfuyReader
from .meteoclimatic_reader import MeteoclimaticReader
from .realtime_reader import RealtimeReader
from .sencrop_reader import SencropReader
from .thingspeak_reader import ThingspeakReader
from .weather_reader import WeatherReader
from .weatherlink_v1_reader import WeatherlinkV1Reader
from .weatherlink_v2_reader import WeatherlinkV2Reader
from .wunderground_reader import WundergroundReader

__all__ = [
    "WeatherReader",
    "WeatherlinkV1Reader",
    "WeatherlinkV2Reader",
    "MeteoclimaticReader",
    "WundergroundReader",
    "ThingspeakReader",
    "HolfuyReader",
    "EcowittReader",
    "RealtimeReader",
    "GoveeReader",
    "SencropReader",
]
