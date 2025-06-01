"""
This module defines the `EcowittReader` class for fetching and parsing weather data
from the Ecowitt API. It processes live and
daily weather data into a standardized `WeatherRecord` format.
"""

import datetime
from gatherer.schema import WeatherRecord, WeatherStation
from .weather_reader import WeatherReader


class EcowittReader(WeatherReader):
    """
    Weather data reader for the Ecowitt API.
    """

    def __init__(
        self, live_endpoint: str, daily_endpoint: str, is_benchmarking: bool = False
    ):
        super().__init__(
            live_endpoint,
            daily_endpoint,
            ignore_early_readings=True,
            is_benchmarking=is_benchmarking,
        )
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
        live_data = data["live"]["data"]
        daily_data = data["daily"]["data"]

        fields = self.get_fields()

        # parse timestamp in seconds
        local_observation_time = (
            datetime.datetime.fromtimestamp(
                self.safe_int(live_data["outdoor"]["temperature"]["time"])
            )
            .replace(tzinfo=station.data_timezone)
            .astimezone(station.local_timezone)
        )

        outdoor = live_data.get("outdoor", {})
        wind = live_data.get("wind", {})
        rainfall = live_data.get("rainfall", {})

        fields["source_timestamp"] = local_observation_time

        fields["live"]["temperature"] = self.safe_float(
            outdoor.get("temperature", {}).get("value")
        )
        fields["live"]["wind_speed"] = self.safe_float(
            wind.get("wind_speed", {}).get("value")
        )
        fields["live"]["wind_direction"] = self.safe_float(
            wind.get("wind_direction", {}).get("value")
        )
        fields["live"]["rain"] = self.safe_float(
            rainfall.get("rain_rate", {}).get("value")
        )
        fields["live"]["humidity"] = self.safe_float(
            outdoor.get("humidity", {}).get("value")
        )
        fields["live"]["pressure"] = self.safe_float(
            live_data.get("pressure", {}).get("relative", {}).get("value")
        )
        fields["live"]["wind_gust"] = self.safe_float(
            wind.get("wind_gust", {}).get("value")
        )

        if fields["source_timestamp"].hour in [0, 1]:
            return fields  # short circuit ignoring the readings from the first 2 hours of the day

        fields["daily"]["max_temperature"] = max(
            self.safe_float(temp)
            for temp in daily_data["outdoor"]["temperature"]["list"].values()
        )
        fields["daily"]["min_temperature"] = min(
            self.safe_float(temp)
            for temp in daily_data["outdoor"]["temperature"]["list"].values()
        )
        fields["daily"]["max_wind_speed"] = max(
            self.safe_float(speed)
            for speed in daily_data["wind"]["wind_speed"]["list"].values()
        )
        fields["daily"]["max_wind_gust"] = max(
            self.safe_float(gust)
            for gust in daily_data["wind"]["wind_gust"]["list"].values()
        )
        fields["daily"]["cumulative_rain"] = self.safe_float(
            rainfall.get("daily", {}).get("value")
        )

        return fields

    def fetch_live_data(self, station: WeatherStation) -> dict:
        """
        Fetch live weather data from the Ecowitt API.

        Args:
            station (WeatherStation): The weather station object.

        Returns:
            dict: The raw live data fetched from the API.
        """
        mac = station.field1
        api_key = station.field2
        application_key = station.field3

        live_url = (
            f"{self.live_endpoint}?mac={mac}&api_key={api_key}"
            f"&application_key={application_key}"
            "&temp_unitid=1&pressure_unitid=3&"
            "wind_speed_unitid=7&rainfall_unitid=12"
        )
        live_response = self.make_request(live_url)
        if live_response:
            return live_response.json()
        return None

    def fetch_daily_data(self, station: WeatherStation) -> dict:
        """
        Fetch daily weather data from the Ecowitt API.

        Args:
            station (WeatherStation): The weather station object.

        Returns:
            dict: The raw daily data fetched from the API.
        """
        mac = station.field1
        api_key = station.field2
        application_key = station.field3

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
            f"&start_date={start_date}"
            f"&end_date={end_date}"
            "&call_back=outdoor.temperature,outdoor.humidity,"
            "wind.wind_speed,wind.wind_gust"
        )
        daily_response = self.make_request(daily_url)
        if daily_response:
            return daily_response.json()
        return None
