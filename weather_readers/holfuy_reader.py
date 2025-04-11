"""
This module defines the `HolfuyReader` class for fetching and parsing weather data
from the Holfuy API. It processes live and daily weather data
into a standardized `WeatherRecord` format.
"""

import datetime
import logging
from datetime import timezone
import json
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
        super().__init__()
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
        try:
            live_data = json.loads(data["live"])
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Invalid JSON data: {e}. Check station connection parameters."
            ) from e
        except KeyError as e:
            raise ValueError(f"Missing expected keys in JSON data: {e}.") from e

        observation_time = datetime.datetime.strptime(
            live_data["dateTime"], "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=station.data_timezone)
        observation_time_utc = observation_time.astimezone(timezone.utc)
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

        wr = WeatherRecord(
            wr_id=None,
            station_id=None,
            source_timestamp=local_observation_time,
            temperature=live_data.get("temperature"),
            wind_speed=live_data.get("wind", {}).get("speed"),
            max_wind_speed=None,
            wind_direction=live_data.get("wind", {}).get("direction"),
            rain=live_data.get("rain"),
            humidity=live_data.get("humidity"),
            pressure=live_data.get("pressure"),
            flagged=False,
            gatherer_thread_id=None,
            cumulative_rain=None,
            max_temperature=None,
            min_temperature=None,
            wind_gust=live_data.get("wind", {}).get("gust"),
            max_wind_gust=None,
        )

        if use_daily:
            wr.min_temperature = live_data.get("daily", {}).get("min_temp")
            wr.max_temperature = live_data.get("daily", {}).get("max_temp")
            wr.cumulative_rain = round(live_data.get("daily", {}).get("sum_rain"), 2)
        else:
            logging.info(
                "Discarding daily data. Observation time: %s, Local time: %s",
                observation_time,
                datetime.datetime.now(tz=station.local_timezone),
            )

        return wr

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

        if live_response.status_code != 200:
            logging.error(
                "Request failed with status code %d. Check station connection parameters.",
                live_response.status_code,
            )
            return None

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

        if daily_response.status_code != 200:
            logging.error(
                "Request failed with status code %d. Check station connection parameters.",
                daily_response.status_code,
            )
            return None

        # daily is deprecated while holfuy fixes their API
        return {"live": live_response.text, "daily": daily_response.text}
