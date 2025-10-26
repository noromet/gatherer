"""
Implementation of a benchmark weather data reader.
"""

import datetime
import time
import zoneinfo

from gatherer.weather_readers import WeatherReader


class BenchmarkWeatherReader(WeatherReader):
    """
    A benchmark weather data reader for testing purposes.
    This class simulates the behavior of a real weather data reader
    but does not fetch any data from an external source.
    """

    def __init__(self, is_benchmarking: bool = False):
        super().__init__(is_benchmarking=is_benchmarking)
        self.required_fields = []

        """
        benchmarking results:

        holfuy: 11 requests, avg=294.13 ms, median=212.05 ms, min=205.20 ms, max=698.40 ms
        wunderground: 105 requests, avg=411.43 ms, median=404.56 ms, min=222.61 ms, max=689.19 ms
        weatherlink_v1: 15 requests, avg=420.56 ms, median=421.61 ms, min=406.71 ms, max=442.31 ms
        realtime: 3 requests, avg=196.11 ms, median=202.44 ms, min=128.91 ms, max=256.97 ms
        meteoclimatic: 7 requests, avg=113.43 ms, median=96.13 ms, min=65.67 ms, max=215.80 ms
        ecowitt: 1 requests, avg=912.12 ms, median=912.12 ms, min=912.12 ms, max=912.12 ms
        weatherlink_v2: 2 requests, avg=836.74 ms, median=836.74 ms, min=831.36 ms, max=842.13 ms
        thingspeak: 1 requests, avg=310.80 ms, median=310.80 ms, min=310.80 ms, max=310.80 ms
        """

        self.times_to_sleep = {
            "holfuy": 0.29413,
            "wunderground": 0.41143,
            "weatherlink_v1": 0.42056,
            "realtime": 0.19611,
            "meteoclimatic": 0.11343,
            "ecowitt": 0.91212,
            "weatherlink_v2": 0.83674,
            "thingspeak": 0.31080,
        }

    def _get_time_to_sleep(self, station):
        """
        Get the time to sleep based on the station name.
        """
        return self.times_to_sleep.get(station.connection_type, 0.2)

    def parse(self, _, __):
        utc_tz = zoneinfo.ZoneInfo("Etc/UTC")
        fields = self.get_fields()
        fields["source_timestamp"] = datetime.datetime.now(
            tz=utc_tz
        ) - datetime.timedelta(seconds=30)
        fields["taken_timestamp"] = datetime.datetime.now(
            tz=utc_tz
        ) - datetime.timedelta(seconds=30)
        return fields

    def fetch_data(self, station):
        """
        Fake data fetching. It imitates the behavior of a real weather data reader
        by sleeping for a short duration before returning.
        """

        time_to_sleep = self._get_time_to_sleep(station)
        if time_to_sleep > 0:
            time.sleep(time_to_sleep)

        return {"live": {}, "daily": {}}
