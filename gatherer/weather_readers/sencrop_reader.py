"""
Implements a reader for the Govee API.
"""

import logging
import datetime
import requests

from gatherer.schema import WeatherRecord, WeatherStation
from .weather_reader import WeatherReader


class SencropReader(WeatherReader):
    """
    Weather data reader for the Sencrop API.
    """

    FIELDS_THAT_MIGHT_HAVE_TIMESTAMP = [
        "WIND_SPEED",
        "WIND_DIRECTION",
        "WIND_GUST",
        "RELATIVE_HUMIDITY",
        "TEMPERATURE",
    ]

    device_data_cache = {}

    def __init__(self, live_endpoint: str, auth_parameters: dict):
        super().__init__(live_endpoint=live_endpoint, auth_parameters=auth_parameters)
        self.required_fields = ["field1"]

    def _get_auth_token(self) -> str:
        """
        curl 'https://api.sencrop.com/v1/oauth2/token' \
            -u '<APPLICATION_ID>:<APPLICATION_SECRET>' \
            -X POST --data '{"grant_type": "client_credentials", "scope": "user"}' \
            -H 'Content-Type: application/json'
        """
        url = f"{self.live_endpoint}/oauth2/token"
        auth = (
            self.auth_parameters.get("SENCROP_APPLICATION_ID"),
            self.auth_parameters.get("SENCROP_APPLICATION_SECRET"),
        )
        headers = {"Content-Type": "application/json"}
        data = {"grant_type": "client_credentials", "scope": "user"}

        response = requests.post(url, auth=auth, headers=headers, json=data)
        response.raise_for_status()
        token_info = response.json()

        token = token_info.get("access_token")

        return token

    def _get_user_id(self, token: str):
        """
        curl 'https://api.sencrop.com/v1/me' \
            -H "Authorization: Bearer <PARTNER_ACCESS_TOKEN>" \
            -L
        """
        url = "https://api.sencrop.com/v1/me"
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        user_info = response.json()

        return user_info.get("item")

    def _list_devices(self, token: str, user_id: str):
        """
        curl 'https://api.sencrop.com/v1/users/1664/devices'  -H "Authorization: Bearer xxxxx"
        """
        url = f"https://api.sencrop.com/v1/users/{user_id}/devices"
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        devices_info = response.json()
        return devices_info

    def parse(self, station: WeatherStation, data: dict) -> WeatherRecord:
        """
        Parse the fetched data into a WeatherRecord object.
        Args:
            station (WeatherStation): The weather station object.
            data (dict): The raw data fetched from the API.
        Returns:
            WeatherRecord: The parsed weather record.
        """
        station_identifier = station.field1
        live_data = data.get("live", {}).get(station_identifier)

        if not live_data:
            logging.error(
                "No live data found for station %s. Check the API response format.",
                station.field1,
            )
            return None
        logging.info("Found live data for station %s.", station.id)

        # find the latest timestamp in the data
        timestamps = []
        latest_timestamp = None
        for key, value in live_data.items():
            if key in SencropReader.FIELDS_THAT_MIGHT_HAVE_TIMESTAMP and isinstance(
                value, dict
            ):
                ts = value.get("date")
                if ts:
                    try:
                        dt = datetime.datetime.fromisoformat(ts)
                        timestamps.append(dt)
                    except ValueError:
                        logging.warning(
                            "Invalid timestamp format for field %s: %s", key, ts
                        )
        if timestamps:
            latest_timestamp = max(timestamps)
        else:
            logging.error(
                "No timestamps found in data for station %s, discarding as invalid.",
                station.id,
            )
            return None

        fields = self.get_fields()
        fields["source_timestamp"] = latest_timestamp
        fields["live"]["temperature"] = live_data.get("TEMPERATURE", {}).get(
            "lastMeasure"
        )
        fields["live"]["humidity"] = live_data.get("RELATIVE_HUMIDITY", {}).get(
            "lastMeasure"
        )
        fields["live"]["wind_speed"] = live_data.get("WIND_SPEED", {}).get(
            "lastMeasure"
        )
        fields["live"]["wind_direction"] = live_data.get("WIND_DIRECTION", {}).get(
            "lastMeasure"
        )
        fields["live"]["wind_gust"] = live_data.get("WIND_GUST", {}).get("lastMeasure")

        return fields

    def fetch_live_data(self, station: WeatherStation) -> dict:
        """Call the Sencrop API and retrieve raw data. Store it in class variable."""
        if len(SencropReader.device_data_cache) == 0:
            logging.info("Fetching all-devices data from Sencrop API.")
            device_id = station.field1
            auth_token = self._get_auth_token()
            if not auth_token:
                logging.error("Failed to retrieve auth token from Sencrop API.")
                return None

            user_id = self._get_user_id(auth_token)

            if not user_id:
                logging.error("Failed to retrieve user ID from Sencrop API.")
                return None

            devices_info = self._list_devices(auth_token, user_id)

            for device_id, device_data in devices_info.get(
                "deviceSummaries", {}
            ).items():
                SencropReader.device_data_cache.setdefault(device_id, device_data)
        else:
            logging.info("Using cached device data for Sencrop API.")

        return SencropReader.device_data_cache
