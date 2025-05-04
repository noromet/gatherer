"""
This module defines the `ThingspeakReader` class for fetching and parsing weather data
from the ThingSpeak API. It processes live weather data into a standardized `WeatherRecord` format.
"""

import datetime
import logging
from schema import WeatherRecord, WeatherStation
from .weather_reader import WeatherReader


class ThingspeakReader(WeatherReader):
    """
    A weather data reader for the ThingSpeak API.

    This class fetches live weather data from the ThingSpeak API
    and parses it into a `WeatherRecord` object. It maps API fields
    to standardized weather parameters and validates timestamps.
    """

    FIELD_MAP = {
        "temperature": "field1",
        "humidity": "field2",
        "pressure": "field4",
    }

    def __init__(self, live_endpoint: str):
        super().__init__(live_endpoint=live_endpoint)
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

        local_observation_time = (
            datetime.datetime.strptime(
                live_data["feeds"][0]["created_at"], "%Y-%m-%dT%H:%M:%SZ"
            )
            .replace(tzinfo=station.data_timezone)
            .astimezone(station.local_timezone)
        )

        fields = self.get_fields()

        fields["source_timestamp"] = local_observation_time

        fields["live"]["temperature"] = self.safe_float(
            live_data.get("feeds")[0].get(self.FIELD_MAP.get("temperature"), None)
        )
        fields["live"]["humidity"] = self.safe_float(
            live_data.get("feeds")[0].get(self.FIELD_MAP.get("humidity"), None)
        )
        fields["live"]["pressure"] = self.safe_float(
            live_data.get("feeds")[0].get(self.FIELD_MAP.get("pressure"), None)
        )

        return fields

    def fetch_live_data(self, station: WeatherStation) -> dict:
        """
        Fetch live weather data from the ThingSpeak API.

        Args:
            station (WeatherStation): The weather station object.

        Returns:
            dict: The raw live data fetched from the API.
        """
        endpoint = f"{self.live_endpoint}/{station.field1}/feeds.json?results=1"

        live_response = self.make_request(endpoint)
        if live_response and live_response.status_code == 200:
            return live_response.json()

        logging.error(
            "Request failed with status code %s. Check station connection parameters.",
            live_response.status_code if live_response else "None",
        )
        return None
