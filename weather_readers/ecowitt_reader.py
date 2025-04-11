"""
This module defines the `EcowittReader` class for fetching and parsing weather data
from the Ecowitt API. It processes live and
daily weather data into a standardized `WeatherRecord` format.
"""

import datetime
from datetime import timezone
import json
from schema import WeatherRecord, WeatherStation
from .weather_reader import WeatherReader


class EcowittReader(WeatherReader):
    """
    Weather data reader for the Ecowitt API.
    """

    def __init__(self, live_endpoint: str, daily_endpoint: str):
        super().__init__()
        self.live_endpoint = live_endpoint
        self.daily_endpoint = daily_endpoint
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
        try:
            live_data = json.loads(data["live"])["data"]
            daily_data = json.loads(data["daily"])["data"]
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Invalid JSON data: {e}. Check station connection parameters."
            ) from e

        # parse timestamp in seconds
        observation_time = datetime.datetime.fromtimestamp(
            self.safe_int(live_data["outdoor"]["temperature"]["time"])
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

        outdoor = live_data.get("outdoor", {})
        wind = live_data.get("wind", {})
        rainfall = live_data.get("rainfall", {})

        temperature = outdoor.get("temperature", {}).get("value")
        wind_speed = wind.get("wind_speed", {}).get("value")
        wind_direction = wind.get("wind_direction", {}).get("value")
        rain = rainfall.get("rain_rate", {}).get("value")
        cumulative_rain = rainfall.get("daily", {}).get("value")
        humidity = outdoor.get("humidity", {}).get("value")
        pressure = live_data.get("pressure", {}).get("relative", {}).get("value")
        wind_gust = wind.get("wind_gust", {}).get("value")

        wr = WeatherRecord(
            wr_id=None,
            station_id=None,
            source_timestamp=local_observation_time,
            temperature=self.safe_float(temperature),
            wind_speed=self.safe_float(wind_speed),
            max_wind_speed=None,
            wind_direction=self.safe_float(wind_direction),
            rain=self.safe_float(rain),
            humidity=self.safe_float(humidity),
            pressure=self.safe_float(pressure),
            flagged=False,
            gatherer_thread_id=None,
            cumulative_rain=self.safe_float(cumulative_rain),
            max_temperature=None,
            min_temperature=None,
            wind_gust=self.safe_float(wind_gust),
            max_wind_gust=None,
        )

        if use_daily:
            max_temperature = max(
                self.safe_float(temp)
                for temp in daily_data["outdoor"]["temperature"]["list"].values()
            )
            min_temperature = min(
                self.safe_float(temp)
                for temp in daily_data["outdoor"]["temperature"]["list"].values()
            )
            max_wind_speed = max(
                self.safe_float(speed)
                for speed in daily_data["wind"]["wind_speed"]["list"].values()
            )
            max_wind_gust = max(
                self.safe_float(gust)
                for gust in daily_data["wind"]["wind_gust"]["list"].values()
            )

            wr.max_temperature = max_temperature
            wr.min_temperature = min_temperature
            wr.max_wind_speed = max_wind_speed
            wr.max_wind_gust = max_wind_gust

        return wr

    def fetch_data(self, station: WeatherStation) -> dict:
        """
        Fetch live and daily weather data from the Ecowitt API.

        Args:
            station (WeatherStation): The weather station object.

        Returns:
            dict: A dictionary containing live and daily weather data.
        """
        mac = station.field1
        api_key = station.field2
        application_key = station.field3

        live_url = (
            f"{self.live_endpoint}?mac={mac}&api_key={api_key}"
            "&application_key={application_key}"
            "&temp_unitid=1&pressure_unitid=3&"
            "wind_speed_unitid=7&rainfall_unitid=12"
        )
        live_response = self.make_request(live_url)

        start_date = datetime.datetime.now().strftime("%Y-%m-%d 00:00:00")
        end_date = datetime.datetime.now().strftime("%Y-%m-%d 23:59:59")
        daily_url = (
            f"{self.daily_endpoint}"
            f"?mac={mac}"
            f"&api_key={api_key}"
            f"&application_key={application_key}"
            "&temp_unitid=1"
            "&pressure_unitid=3"
            "&wind_speed_unitid=7"
            "&rainfall_unitid=12"
            f"&cycle_type=auto"
            ""
            f"&start_date={start_date}"
            f"&end_date={end_date}"
            "&call_back=outdoor.temperature,outdoor.humidity,"
            "wind.wind_speed,wind.wind_gust"
        )
        daily_response = self.make_request(daily_url)

        return {"live": live_response.text, "daily": daily_response.text}
