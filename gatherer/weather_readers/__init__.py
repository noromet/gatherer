"""
This module holds the WeatherReader classes for various weather data sources.
"""

from gatherer.schema import WeatherRecord

from .meteoclimatic_reader import MeteoclimaticReader
from .weatherlink_v1_reader import WeatherlinkV1Reader
from .wunderground_reader import WundergroundReader
from .weatherlink_v2_reader import WeatherlinkV2Reader
from .thingspeak_reader import ThingspeakReader
from .holfuy_reader import HolfuyReader
from .ecowitt_reader import EcowittReader
from .realtime_reader import RealtimeReader
from .weather_reader import WeatherReader
