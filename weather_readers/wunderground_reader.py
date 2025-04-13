"""
This module defines the `WundergroundReader` class for fetching and parsing weather data
from the Weather Underground API. It processes live and daily weather data into a standardized
`WeatherRecord` format.
"""

import datetime
import logging
import requests
from schema import WeatherRecord, WeatherStation
from .weather_reader import WeatherReader


class WundergroundReader(WeatherReader):
    """
    A weather data reader for the Weather Underground API.

    This class fetches live and daily weather data from the Weather Underground API
    and parses it into a `WeatherRecord` object. It validates timestamps and handles
    data transformation for various weather parameters.
    """

    def __init__(self, live_endpoint: str, daily_endpoint: str):
        super().__init__(ignore_early_readings=True)
        self.live_endpoint = live_endpoint
        self.daily_endpoint = daily_endpoint
        self.required_fields = ["field1", "field2"]

    def parse(self, station: WeatherStation, data: dict) -> WeatherRecord:
        """
        Parse the fetched data into a WeatherRecord object.

        Args:
            station (WeatherStation): The weather station object.
            data (dict): The raw data fetched from the API.

        Returns:
            WeatherRecord: The parsed weather record.
        """

        live_data = data["live"]["observations"][0]
        daily_data = data["daily"]["summaries"][-1]

        # datetime management
        local_observation_time = (
            datetime.datetime.strptime(live_data["obsTimeLocal"], "%Y-%m-%d %H:%M:%S")
            .replace(tzinfo=station.data_timezone)
            .astimezone(station.local_timezone)
        )

        fields = self.get_fields()

        fields["source_timestamp"] = local_observation_time

        live_metric_data = live_data.get("metric")  # esquizo
        if live_metric_data is None:
            raise ValueError("No metric data found in live data.")

        fields["instant"]["temperature"] = live_metric_data.get("temp", None)
        fields["instant"]["wind_speed"] = live_metric_data.get("windSpeed", None)
        fields["instant"]["wind_direction"] = live_data.get("winddir", None)
        fields["instant"]["rain"] = live_metric_data.get("precipRate", None)
        fields["instant"]["humidity"] = live_data.get("humidity", None)
        fields["instant"]["pressure"] = live_metric_data.get("pressure", None)
        fields["instant"]["wind_gust"] = live_metric_data.get("windGust", None)

        fields["daily"]["cumulative_rain"] = live_metric_data.get("precipTotal", None)
        daily_metric_data = daily_data.get("metric")

        fields["daily"]["max_wind_speed"] = daily_metric_data.get("windspeedHigh", None)
        fields["daily"]["max_temperature"] = daily_metric_data.get("tempHigh", None)
        fields["daily"]["min_temperature"] = daily_metric_data.get("tempLow", None)
        fields["daily"]["max_wind_gust"] = daily_metric_data.get("windgustHigh", None)

        return fields

    def fetch_data(self, station: WeatherStation) -> dict:
        """
        Fetch live and daily weather data from the Weather Underground API.

        Args:
            station (WeatherStation): The weather station object.

        Returns:
            dict: A dictionary containing live and daily weather data.
        """
        did, token = station.field1, station.field2

        live_response = requests.get(
            self.live_endpoint,
            {
                "stationId": did,
                "apiKey": token,
                "format": "json",
                "units": "m",
                "numericPrecision": "decimal",
            },
        )
        logging.info("Requesting %s", live_response.url)

        daily_response = requests.get(
            self.daily_endpoint,
            {
                "stationId": did,
                "apiKey": token,
                "format": "json",
                "units": "m",
                "numericPrecision": "decimal",
            },
        )
        logging.info("Requesting %s", daily_response.url)

        return {"live": live_response.json(), "daily": daily_response.json()}
