"""
This module defines the `MeteoclimaticReader` class for fetching and parsing weather data
from the Meteoclimatic API.
It processes live weather data into a standardized `WeatherRecord` format.
"""

import requests

from gatherer.schema import WeatherRecord, WeatherStation

from .weather_reader import WeatherReader


class MeteoclimaticReader(WeatherReader):
    """
    Weather data reader for the Meteoclimatic API.
    """

    def __init__(self, is_benchmarking: bool = False):
        super().__init__(is_benchmarking=is_benchmarking)
        self.required_fields = ["field1"]

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

        if not live_data:
            raise ValueError("No data received from the station.")

        data = {}
        for line in live_data.strip().split("*"):
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, value = line.split("=")
            key = key.strip()
            value = value.strip()

            if key in CODE_TO_NAME and key in WHITELIST:
                data[CODE_TO_NAME[key]] = value

        data["record_timestamp"] = self.smart_parse_datetime(
            data["record_timestamp"], timezone=station.data_timezone
        )
        if data["record_timestamp"] is None:
            raise ValueError("Cannot accept a reading without a timestamp.")

        local_observation_time = (
            data["record_timestamp"]
            .replace(tzinfo=station.data_timezone)
            .astimezone(station.local_timezone)
        )

        fields = self.get_fields()

        fields["source_timestamp"] = local_observation_time

        fields["live"]["wind_direction"] = self.smart_azimuth(
            data.get("current_wind_direction", None)
        )

        temperature = self.smart_parse_float(
            data.get("current_temperature_celsius", None)
        )
        fields["live"]["temperature"] = None if temperature == 100 else temperature
        wind_speed = self.smart_parse_float(data.get("current_wind_speed_kph", None))
        fields["live"]["wind_speed"] = None if wind_speed == 100 else wind_speed

        wind_direction = self.smart_azimuth(data.get("current_wind_direction", None))
        fields["live"]["wind_direction"] = (
            None if wind_direction == 100 else wind_direction
        )

        humidity = self.smart_parse_float(data.get("relative_humidity", None))
        fields["live"]["humidity"] = None if humidity == 100 else humidity

        pressure = self.smart_parse_float(data.get("pressure_hpa", None))
        fields["live"]["pressure"] = None if pressure == 100 else pressure

        cumulative_rain = self.smart_parse_float(
            data.get("total_daily_precipitation_at_record_timestamp", None)
        )
        fields["daily"]["cumulative_rain"] = (
            None if cumulative_rain == 100 else cumulative_rain
        )

        max_temperature = self.smart_parse_float(
            data.get("daily_max_temperature", None)
        )
        fields["daily"]["max_temperature"] = (
            None if max_temperature == 100 else max_temperature
        )

        min_temperature = self.smart_parse_float(
            data.get("daily_min_temperature", None)
        )
        fields["daily"]["min_temperature"] = (
            None if min_temperature == 100 else min_temperature
        )

        max_wind_gust = self.smart_parse_float(data.get("daily_max_wind_gust", None))
        fields["daily"]["max_wind_gust"] = (
            None if max_wind_gust == 100 else max_wind_gust
        )

        return fields

    def fetch_live_data(self, station: WeatherStation) -> dict:
        """
        Fetch live weather data from a Meteoclimatic endpoint.

        Args:
            station (WeatherStation): The weather station object.

        Returns:
            dict: The raw live data fetched from the endpoint.
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/58.0.3029.110 Safari/537.3"
        }

        endpoint = station.field1
        response = self.make_request(endpoint, headers=headers)

        if not response or response.status_code != 200:
            code = response.status_code if response else "None"
            raise requests.exceptions.HTTPError(
                f"Error: Received status code {code} from {endpoint}"
            )

        return response.text


CODE_TO_NAME = {
    "VER": "version",
    "COD": "station_code",
    "SIG": "signature",
    "UPD": "record_timestamp",
    "TMP": "current_temperature_celsius",
    "WND": "current_wind_speed_kph",
    "AZI": "current_wind_direction",
    "BAR": "pressure_hpa",
    "HUM": "relative_humidity",
    "SUN": "solar_radiation_index",
    "UVI": "uva_index",
    "DHTM": "daily_max_temperature",
    "DLTM": "daily_min_temperature",
    "DHHM": "daily_max_humidity",
    "DLHM": "daily_min_humidity",
    "DHBR": "daily_max_pressure",
    "DLBR": "daily_min_pressure",
    "DGST": "daily_max_wind_gust",
    "DSUN": "daily_max_solar_radiation_index",
    "DHUV": "daily_max_uva_index",
    "DPCP": "total_daily_precipitation_at_record_timestamp",
    "WRUN": "wind_run_distance_daily",
    "MHTM": "monthly_max_temperature",
    "MLTM": "monthly_min_temperature",
    "MHHM": "monthly_max_humidity",
    "MLHM": "monthly_min_humidity",
    "MHBR": "monthly_max_pressure",
    "MLBR": "monthly_min_pressure",
    "MSUN": "monthly_max_solar_index",
    "MHUV": "monthly_max_uva_index",
    "MGST": "monthly_max_wind_speed",
    "MPCP": "total_precipitation_current_month",
    "YHTM": "yearly_max_temperature",
    "YLTM": "yearly_min_temperature",
    "YHHM": "yearly_max_humidity",
    "YLHM": "yearly_min_humidity",
    "YHBR": "yearly_max_pressure",
    "YLBR": "yearly_min_pressure",
    "YGST": "yearly_max_wind_speed",
    "YSUN": "yearly_max_solar_index",
    "YHUV": "yearly_max_uva_index",
    "YPCP": "total_precipitation_current_year",
}

WHITELIST = [
    "UPD",
    "TMP",
    "WND",
    "DGST",  # daily max wind speed: max_wind_speed
    "AZI",
    "DPCP",  # lluvia cumulativa
    "HUM",
    "BAR",
    "DHTM",
    "DLTM",
]
