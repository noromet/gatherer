"""
This package holds the WeatherReader classes for various weather data sources.
"""

from .meteoclimatic_reader import MeteoclimaticReader
from .weatherlink_v1_reader import WeatherlinkV1Reader
from .wunderground_reader import WundergroundReader
from .weatherlink_v2_reader import WeatherlinkV2Reader
from .thingspeak_reader import ThingspeakReader
from .holfuy_reader import HolfuyReader
from .ecowitt_reader import EcowittReader
from .realtime_reader import RealtimeReader
from .weather_reader import WeatherReader
from .govee_reader import GoveeReader

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
]
