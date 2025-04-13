"""
Collection of factory methods to create test data for WeatherRecord.
"""

import uuid
import datetime
from schema import WeatherRecord  # Adjust the import path as needed


def create_weather_record(**overrides):
    """Factory method to create a WeatherRecord with default values."""
    defaults = {
        "wr_id": uuid.uuid4(),
        "station_id": uuid.uuid4(),
        "source_timestamp": datetime.datetime.now(),
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
