"""
Implements a reader for the Govee API.
"""

import datetime
import logging

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
        self.auth_token: str = self._get_auth_token()
        self.user_id: str = self._get_user_id(self.auth_token)
        self.devices_info = self._list_devices(self.auth_token, self.user_id)

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
        url = f"{self.live_endpoint}/me"
        headers = {"Authorization": f"Bearer {token}"}
        response = self.make_request(url, headers=headers)
        user_info = response.json()

        return user_info.get("item")

    def _list_devices(self, token: str, user_id: str):
        """
        curl 'https://api.sencrop.com/v1/users/1664/devices'  -H "Authorization: Bearer xxxxx"
        """
        url = f"{self.live_endpoint}/users/{user_id}/devices"
        headers = {"Authorization": f"Bearer {token}"}
        response = self.make_request(url, headers=headers)
        devices_info = response.json()
        return devices_info

    def _parse_live_data(self, station: WeatherStation, live_data: dict) -> tuple:
        """
        Parse live weather data and extract the latest timestamp.

        Args:
            station: The weather station object
            live_data: Live data from the API

        Returns:
            tuple: (latest_timestamp, live_fields_dict) or (None, None) if invalid
        """
        if not live_data:
            logging.error(
                "No live data found for station %s. Check the API response format.",
                station.field1,
            )
            return None, None

        logging.info("Found live data for station %s.", station.id)

        # find the latest timestamp in the data
        timestamps = []
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

        if not timestamps:
            logging.error(
                "No timestamps found in data for station %s, discarding as invalid.",
                station.id,
            )
            return None, None

        latest_timestamp = max(timestamps)

        live_fields = {
            "temperature": live_data.get("TEMPERATURE", {}).get("lastMeasure"),
            "humidity": live_data.get("RELATIVE_HUMIDITY", {}).get("lastMeasure"),
            "wind_speed": live_data.get("WIND_SPEED", {}).get("lastMeasure"),
            "wind_direction": live_data.get("WIND_DIRECTION", {}).get("lastMeasure"),
            "wind_gust": live_data.get("WIND_GUST", {}).get("lastMeasure"),
        }

        return latest_timestamp, live_fields

    def _parse_daily_data(self, station: WeatherStation, daily_data: list) -> dict:
        """
        Parse daily weather data and calculate cumulative rain.

        Args:
            station: The weather station object
            daily_data: List of daily data entries from the API

        Returns:
            dict: Daily fields dict or None if no valid data
        """
        if not daily_data:
            return {"cumulative_rain": None}

        # Filter entries for the current natural day in station's timezone
        station_tz = station.data_timezone
        now_in_tz = datetime.datetime.now(tz=station_tz)
        start_of_day = datetime.datetime(
            year=now_in_tz.year,
            month=now_in_tz.month,
            day=now_in_tz.day,
            tzinfo=station_tz,
        )
        end_of_day = start_of_day + datetime.timedelta(days=1)

        logging.debug(
            "Filtering daily data for station %s (timezone %s) between %s and %s.",
            station.id,
            station_tz,
            start_of_day,
            end_of_day,
        )

        cumulative_rain = 0
        for entry in daily_data:
            entry_key = entry.get("key")
            if entry_key:
                entry_dt = datetime.datetime.fromtimestamp(
                    entry_key / 1000, tz=station_tz
                )
                if start_of_day < entry_dt < end_of_day:
                    rain_value = entry.get("RAIN_FALL_MEAN_SUM_ADJUSTED", {}).get(
                        "value", 0
                    )
                    # print("using")
                    # print(entry_dt)
                    # print(entry)
                    # print("\n")
                    # print(f"addition: {cumulative_rain} + {rain_value}
                    #       = {cumulative_rain + rain_value}")

                    cumulative_rain += rain_value

        return {"cumulative_rain": cumulative_rain}

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
        daily_data = data.get("daily", {})

        # Parse live data
        latest_timestamp, live_fields = self._parse_live_data(station, live_data)

        # Parse daily data
        daily_fields = self._parse_daily_data(station, daily_data)

        # Combine results
        fields = self.get_fields()
        fields["source_timestamp"] = latest_timestamp
        fields["live"].update(live_fields)
        fields["daily"].update(daily_fields)

        return fields

    def fetch_live_data(self, station: WeatherStation) -> dict:
        """
        Call the Sencrop API and retrieve raw data.
        Store it in class variable.
        THIS ONE IS ONLY CALLED ONCE.
        """
        if len(SencropReader.device_data_cache) == 0:
            logging.info("Caching all-devices data.")

            for device_id, device_data in self.devices_info.get(
                "deviceSummaries", {}
            ).items():
                SencropReader.device_data_cache.setdefault(device_id, device_data)
        else:
            logging.info("Using cached Sencrop API data for device %s.", station.id)

        return SencropReader.device_data_cache

    def fetch_daily_data(self, station: WeatherStation) -> dict:
        """
        Fetch daily data for the given station.
        Args:
            station (WeatherStation): The weather station object.
        Returns:
            dict: The raw daily data fetched from the API.

        THIS ONE IS CALLED FOR EACH STATION.
        """
        device_id = station.field1
        if not self.auth_token:
            logging.error("Failed to retrieve auth token from Sencrop API.")
            return None

        url = (
            f"{self.live_endpoint}/users/{self.user_id}/devices/{device_id}/data/hourly"
        )
        headers = {"Authorization": f"Bearer {self.auth_token}"}
        params = {
            "beforeDate": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "days": "1",
            "measures": "RAIN_FALL,RAIN_FALL_MEAN_SUM,RAIN_FALL_MEAN_SUM_ADJUSTED",
            "timeZone": "Etc/UTC",
        }
        response = self.make_request(url, params=params, headers=headers)

        if not response:
            return None

        datalist = response.json().get("measures", {}).get("data", {})

        if not datalist:
            logging.warning(
                "No daily data found for station %s. Check the API response format.",
                station.id,
            )
            return None

        return datalist
