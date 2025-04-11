"""
This module defines the `WundergroundReader` class for fetching and parsing weather data
from the Weather Underground API. It processes live and daily weather data into a standardized
`WeatherRecord` format.
"""

import datetime
import logging
from datetime import timezone
import json
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
        super().__init__()
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
        try:
            live_data = json.loads(data["live"])["observations"][0]
            daily_data = json.loads(data["daily"])["summaries"][-1]
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Invalid JSON data: {e}. Check station connection parameters."
            ) from e

        # datetime management
        observation_time = datetime.datetime.strptime(
            live_data["obsTimeLocal"], "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=station.data_timezone)
        observation_time_utc = datetime.datetime.strptime(
            live_data["obsTimeUtc"], "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=timezone.utc)
        self.assert_date_age(observation_time_utc)

        local_observation_time = observation_time.astimezone(station.local_timezone)
        current_date = datetime.datetime.now(tz=station.data_timezone).date()
        observation_date = observation_time.date()
        if (
            observation_time.time() >= datetime.time(0, 0)
            and observation_time.time() <= datetime.time(0, 15)
            and observation_date == current_date
        ):
            use_daily = False
        else:
            use_daily = True

        live_metric_data = live_data.get("metric")  # esquizo
        if live_metric_data is None:
            raise ValueError("No metric data found in live data.")

        temperature = live_metric_data.get("temp", None)
        wind_speed = live_metric_data.get("windSpeed", None)
        wind_direction = live_data.get("winddir", None)
        rain = live_metric_data.get("precipRate", None)
        cumulative_rain = live_metric_data.get("precipTotal", None)
        humidity = live_data.get("humidity", None)
        pressure = live_metric_data.get("pressure", None)
        wind_gust = live_metric_data.get("windGust", None)

        wr = WeatherRecord(
            wr_id=None,
            station_id=None,
            source_timestamp=local_observation_time,
            temperature=temperature,
            wind_speed=wind_speed,
            max_wind_speed=None,
            wind_direction=wind_direction,
            rain=rain,
            humidity=humidity,
            pressure=pressure,
            flagged=False,
            gatherer_thread_id=None,
            cumulative_rain=cumulative_rain,
            max_temperature=None,
            min_temperature=None,
            wind_gust=wind_gust,
            max_wind_gust=None,
        )

        if use_daily:
            daily_metric_data = daily_data.get("metric")

            max_wind_speed = daily_metric_data.get("windspeedHigh", None)
            max_temperature = daily_metric_data.get("tempHigh", None)
            min_temperature = daily_metric_data.get("tempLow", None)
            max_wind_gust = daily_metric_data.get("windgustHigh", None)

            wr.max_wind_speed = max_wind_speed
            wr.max_temperature = max_temperature
            wr.min_temperature = min_temperature
            wr.max_wind_gust = max_wind_gust
        else:
            logging.warning(
                "Discarding daily data. Observation time: %s, Local time: %s",
                observation_time,
                datetime.datetime.now(tz=station.local_timezone),
            )

        return wr

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

        if live_response.status_code != 200:
            logging.error(
                "Request failed with status code %d. Check station connection parameters.",
                live_response.status_code,
            )
            return None

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

        if daily_response.status_code != 200:
            logging.error(
                "Request failed with status code %d. Check station connection parameters.",
                daily_response.status_code,
            )
            return None

        return {"live": live_response.text, "daily": daily_response.text}
