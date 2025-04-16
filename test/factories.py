"""
Collection of factory methods to create test data for WeatherRecord.
"""

import uuid
import datetime
from schema import WeatherRecord, WeatherStation
from gatherer import weather_readers


def create_weather_record(**overrides):
    """Factory method to create a WeatherRecord with default values."""
    defaults = {
        "wr_id": uuid.uuid4(),
        "station_id": uuid.uuid4(),
        "source_timestamp": datetime.datetime.now(),
        "taken_timestamp": datetime.datetime.now(),
        "temperature": 0.0,
        "wind_speed": 10.0,
        "max_wind_speed": 20.0,
        "wind_direction": 180.0,
        "rain": 5.0,
        "humidity": 50.0,
        "pressure": 1013.0,
        "flagged": False,
        "gatherer_thread_id": uuid.uuid4(),
        "cumulative_rain": 10.0,
        "max_temperature": 30.0,
        "min_temperature": 20.0,
        "wind_gust": 15.0,
        "max_wind_gust": 25.0,
    }
    defaults.update(overrides)
    return WeatherRecord(**defaults)


def create_weather_station(**overrides):
    """Factory method to create a WeatherStation with default values."""
    defaults = {
        "ws_id": uuid.uuid4(),
        "connection_type": "mock",
        "field1": "value1",
        "field2": "value2",
        "field3": "value3",
        "pressure_offset": 0.0,
        "data_timezone": "Etc/UTC",
        "local_timezone": "Etc/UTC",
    }
    defaults.update(overrides)
    return WeatherStation(**defaults)


def create_weather_reader(reader_cls, **overrides):
    """
    Factory method to create a weather reader instance with default values.
    Usage: reader = create_weather_reader(EcowittReader)
    """

    defaults = {}

    if reader_cls is weather_readers.WundergroundReader:
        defaults = {
            "live_endpoint": "example.com",
            "daily_endpoint": "example.com",
        }
    elif reader_cls is weather_readers.EcowittReader:
        defaults = {
            "live_endpoint": "example.com",
            "daily_endpoint": "example.com",
        }
    elif reader_cls is weather_readers.WeatherLinkV1Reader:
        defaults = {
            "live_endpoint": "example.com",
        }
    elif reader_cls is weather_readers.WeatherlinkV2Reader:
        defaults = {
            "live_endpoint": "example.com",
            "daily_endpoint": "example.com",
        }
    elif reader_cls is weather_readers.ThingspeakReader:
        defaults = {
            "live_endpoint": "example.com",
        }
    elif reader_cls is weather_readers.HolfuyReader:
        defaults = {
            "live_endpoint": "example.com",
            "daily_endpoint": "example.com",
        }
    elif reader_cls is weather_readers.MeteoclimaticReader:
        # MeteoclimaticReader takes no arguments
        return reader_cls(**overrides)
    elif reader_cls is weather_readers.RealtimeReader:
        # RealtimeReader takes no arguments
        return reader_cls(**overrides)
    else:
        # fallback for unknown readers
        return reader_cls(**overrides)

    defaults.update(overrides)
    return reader_cls(**defaults)
