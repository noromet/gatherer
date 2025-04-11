"""
This module defines the `MeteoclimaticReader` class for fetching and parsing weather data
from the Meteoclimatic API.
It processes live weather data into a standardized `WeatherRecord` format.
"""

import json
import logging
from datetime import timezone
import requests
from schema import WeatherRecord, WeatherStation
from .weather_reader import WeatherReader


class MeteoclimaticReader(WeatherReader):
    """
    Weather data reader for the Meteoclimatic API.
    """

    def __init__(self):
        super().__init__()
        self.required_fields = ["field1"]

    def check_var_for_100(self, var, var_name, station_id, data):
        """
        Check if a variable is equal to 100 and log an error if it is.
        This is an error specific to Meteoclimatic data.
        """
        if var == 100:
            logging.error(
                "[%s]: %s == 100: %s. Dump: %s",
                station_id,
                var_name,
                var,
                json.dumps(data),
            )

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

        observation_time_utc = data["record_timestamp"].astimezone(timezone.utc)
        self.assert_date_age(observation_time_utc)

        local_observation_time = observation_time_utc.astimezone(station.local_timezone)

        try:
            wind_direction = self.smart_azimuth(
                data.get("current_wind_direction", None)
            )

            temperature = self.smart_parse_float(
                data.get("current_temperature_celsius", None)
            )
            self.check_var_for_100(
                var=temperature,
                var_name="Temperature",
                station_id=station.id,
                data=data,
            )

            wind_speed = self.smart_parse_float(
                data.get("current_wind_speed_kph", None)
            )
            self.check_var_for_100(
                var=wind_speed, var_name="Wind Speed", station_id=station.id, data=data
            )

            max_wind_gust = self.smart_parse_float(
                data.get("daily_max_wind_gust", None)
            )
            self.check_var_for_100(
                var=max_wind_gust,
                var_name="Max Wind Gust",
                station_id=station.id,
                data=data,
            )

            cumulative_rain = self.smart_parse_float(
                data.get("total_daily_precipitation_at_record_timestamp", None)
            )
            self.check_var_for_100(
                var=cumulative_rain,
                var_name="Cumulative Rain",
                station_id=station.id,
                data=data,
            )

            humidity = self.smart_parse_float(data.get("relative_humidity", None))
            self.check_var_for_100(
                var=humidity, var_name="Humidity", station_id=station.id, data=data
            )

            pressure = self.smart_parse_float(data.get("pressure_hpa", None))
            self.check_var_for_100(
                var=pressure, var_name="Pressure", station_id=station.id, data=data
            )

            max_temperature = self.smart_parse_float(
                data.get("daily_max_temperature", None)
            )
            self.check_var_for_100(
                var=max_temperature,
                var_name="Max Temperature",
                station_id=station.id,
                data=data,
            )

            min_temperature = self.smart_parse_float(
                data.get("daily_min_temperature", None)
            )
            self.check_var_for_100(
                var=min_temperature,
                var_name="Min Temperature",
                station_id=station.id,
                data=data,
            )

            wr = WeatherRecord(
                wr_id=None,
                station_id=station.id,
                source_timestamp=local_observation_time,
                temperature=temperature,
                wind_speed=wind_speed,
                max_wind_speed=None,
                wind_direction=wind_direction,
                rain=None,
                humidity=humidity,
                pressure=pressure,
                flagged=False,
                gatherer_thread_id=None,
                cumulative_rain=cumulative_rain,
                max_temperature=max_temperature,
                min_temperature=min_temperature,
                wind_gust=None,
                max_wind_gust=max_wind_gust,
            )

            return wr
        except KeyError as e:
            raise ValueError(f"Missing key {e} in data.") from e

    def fetch_data(self, station: WeatherStation) -> dict:
        """
        Fetch live weather data from the Meteoclimatic API.

        Args:
            station (WeatherStation): The weather station object.

        Returns:
            dict: A dictionary containing live weather data.
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/58.0.3029.110 Safari/537.3"
        }

        response = self.make_request(station.field1, headers=headers)

        if response.status_code != 200:
            raise requests.exceptions.HTTPError(
                f"Error: Received status code {response.status_code} from {station.field1}"
            )

        return {"live": response.text}


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
