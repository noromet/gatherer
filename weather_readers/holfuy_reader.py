"""
This module defines the `HolfuyReader` class for fetching and parsing weather data
from the Holfuy API. It processes live and daily weather data
into a standardized `WeatherRecord` format.
"""

import datetime
from schema import WeatherRecord, WeatherStation
from .weather_reader import WeatherReader


class HolfuyReader(WeatherReader):
    """
    A weather data reader for the Holfuy API.

    This class fetches live and daily weather data from the Holfuy API
    and parses it into a `WeatherRecord` object. It validates timestamps
    and handles data transformation for various weather parameters.
    """

    def __init__(self, live_endpoint: str, daily_endpoint: str):
        super().__init__(ignore_early_readings=True)
        self.live_endpoint = live_endpoint
        self.daily_endpoint = daily_endpoint
        self.required_fields = ["field1", "field3"]  # station_id and password

    def parse(self, station: WeatherStation, data: dict) -> WeatherRecord:
        """
        Parse the fetched data into a WeatherRecord object.

        Args:
            station (WeatherStation): The weather station object.
            data (dict): The raw data fetched from the API.

        Returns:
            WeatherRecord: The parsed weather record.
        """
        live_data = data["live"]

        fields = self.get_fields()

        local_observation_time = (
            datetime.datetime.strptime(live_data["dateTime"], "%Y-%m-%d %H:%M:%S")
            .replace(tzinfo=station.data_timezone)
            .astimezone(station.local_timezone)
        )

        fields["source_timestamp"] = local_observation_time

        fields["instant"]["temperature"] = self.safe_float(live_data.get("temperature"))
        fields["instant"]["wind_speed"] = self.safe_float(
            live_data.get("wind", {}).get("speed")
        )
        fields["instant"]["wind_direction"] = self.safe_float(
            live_data.get("wind", {}).get("direction")
        )
        fields["instant"]["rain"] = self.safe_float(live_data.get("rain"))
        fields["instant"]["humidity"] = self.safe_float(live_data.get("humidity"))
        fields["instant"]["pressure"] = self.safe_float(live_data.get("pressure"))
        fields["instant"]["wind_gust"] = self.safe_float(
            live_data.get("wind", {}).get("gust")
        )

        fields["daily"]["max_temperature"] = self.safe_float(
            live_data.get("daily", {}).get("max_temp")
        )
        fields["daily"]["min_temperature"] = self.safe_float(
            live_data.get("daily", {}).get("min_temp")
        )
        fields["daily"]["max_wind_speed"] = self.safe_float(
            live_data.get("daily", {}).get("max_wind_speed")
        )
        fields["daily"]["max_wind_gust"] = self.safe_float(
            live_data.get("daily", {}).get("max_wind_gust")
        )
        fields["daily"]["cumulative_rain"] = self.safe_float(
            live_data.get("daily", {}).get("sum_rain")
        )

        return fields

    def fetch_data(self, station: WeatherStation) -> dict:
        """
        Fetch live and daily weather data from the Holfuy API.

        Args:
            station (WeatherStation): The weather station object.

        Returns:
            dict: A dictionary containing live and daily weather data.
        """
        station_id, password = station.field1, station.field3

        live_url = (
            f"{self.live_endpoint}?s={station_id}"
            f"&pw={password}"
            "&m=JSON"
            "&tu=C"
            "&su=km/h"
            "&daily=True"
        )
        live_response = self.make_request(live_url)

        daily_url = (
            f"{self.daily_endpoint}?s={station_id}"
            f"&pw={password}"
            "&m=JSON&"
            "tu=C"
            "&su=km/h"
            "&type=2"
            "&mback=60"
        )
        daily_response = self.make_request(daily_url)

        # daily is deprecated while holfuy fixes their API
        return {"live": live_response.json(), "daily": daily_response.json()}
