"""
This module defines the `WeatherlinkV1Reader` class
for fetching and parsing weather datan from the WeatherLink V1 API.
It processes live weather data into a standardized `WeatherRecord` format.
"""

import datetime
import logging
from gatherer.schema import WeatherRecord, WeatherStation
from .utils import UnitConverter
from .weather_reader import WeatherReader


class WeatherlinkV1Reader(WeatherReader):
    """
    A weather data reader for the WeatherLink V1 API.

    This class fetches live weather data from the WeatherLink V1 API
    and parses it into a `WeatherRecord` object. It validates timestamps
    and handles data transformation for various weather parameters.
    """

    def __init__(self, live_endpoint: str):
        super().__init__(live_endpoint=live_endpoint, ignore_early_readings=True)
        self.required_fields = ["field1", "field2", "field3"]

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
            datetime.datetime.strptime(
                live_data["observation_time_rfc822"], "%a, %d %b %Y %H:%M:%S %z"
            )
            .replace(tzinfo=station.data_timezone)
            .astimezone(station.local_timezone)
        )

        fields["source_timestamp"] = local_observation_time

        fields["live"]["temperature"] = self.safe_float(live_data.get("temp_c"))
        fields["live"]["wind_speed"] = UnitConverter.mph_to_kph(
            self.safe_float(live_data.get("wind_mph"))
        )
        fields["live"]["wind_direction"] = self.safe_float(
            live_data.get("wind_degrees")
        )
        fields["live"]["rain"] = UnitConverter.inches_to_mm(
            self.safe_float(
                live_data.get("davis_current_observation").get("rain_rate_in_per_hr")
            )
        )
        fields["live"]["humidity"] = self.safe_float(live_data.get("relative_humidity"))
        fields["live"]["pressure"] = self.safe_float(live_data.get("pressure_mb"))
        fields["live"]["wind_gust"] = UnitConverter.mph_to_kph(
            self.safe_float(
                live_data["davis_current_observation"].get("wind_ten_min_gust_mph")
            )
        )

        fields["daily"]["max_wind_speed"] = UnitConverter.mph_to_kph(
            self.safe_float(
                live_data["davis_current_observation"].get("wind_day_high_mph")
            )
        )
        fields["daily"]["max_temperature"] = UnitConverter.fahrenheit_to_celsius(
            self.safe_float(
                live_data["davis_current_observation"].get("temp_day_high_f")
            )
        )
        fields["daily"]["min_temperature"] = UnitConverter.fahrenheit_to_celsius(
            self.safe_float(
                live_data["davis_current_observation"].get("temp_day_low_f")
            )
        )
        fields["daily"]["cumulative_rain"] = UnitConverter.inches_to_mm(
            self.safe_float(live_data["davis_current_observation"].get("rain_day_in"))
        )

        return fields

    def fetch_live_data(self, station: WeatherStation) -> dict:
        """
        Fetch live weather data from the WeatherLink V1 API.

        Args:
            station (WeatherStation): The weather station object.

        Returns:
            dict: The raw live data fetched from the API.
        """
        user, api_token, password = station.field1, station.field2, station.field3

        params = {"user": user, "pass": password, "apiToken": api_token}
        live_response = self.make_request(self.live_endpoint, params=params)

        if live_response and live_response.status_code == 200:
            return live_response.json()

        logging.error(
            "Request failed for station %s. Check station connection parameters.",
            station.id,
        )
        return None
