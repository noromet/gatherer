"""
RealtimeReader
This class is responsible for reading weather data from a real-time weather station.
It fetches data from a specified endpoint and parses it into a WeatherRecord object.
"""

from gatherer.schema import WeatherRecord, WeatherStation
from .weather_reader import WeatherReader


class RealtimeReader(WeatherReader):
    """
    Weather data reader for realtime.txt protocol weather stations.
    """

    INDEX_TO_DATA = {
        0: "date",
        1: "time",
        2: "current_temperature_celsius",
        3: "relative_humidity",
        5: "current_wind_speed_kph",
        6: "last_wind_speed_kph",
        7: "current_wind_direction",
        8: "rain_rate_mm",
        9: "total_daily_precipitation_at_record_timestamp",
        10: "pressure_hpa",
        30: "daily_max_temperature",
        28: "daily_min_temperature",
        32: "daily_max_wind_speed",
    }

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
        valid_keys = self.INDEX_TO_DATA.keys()
        temp_dict = {}
        for index, item in enumerate(data["live"].strip().split(" ")):
            item = item.strip()
            if not item:
                continue
            if index in valid_keys:
                temp_dict[self.INDEX_TO_DATA[index]] = item
            index += 1

        _date = self.smart_parse_datetime(
            f"{temp_dict['date']} {temp_dict['time']}", timezone=station.data_timezone
        )

        local_observation_time = _date.replace(tzinfo=station.data_timezone).astimezone(
            station.local_timezone
        )

        fields = self.get_fields()

        fields["source_timestamp"] = local_observation_time

        fields["live"]["wind_direction"] = self.safe_float(
            temp_dict.get("current_wind_direction", None)
        )

        fields["live"]["temperature"] = self.safe_float(
            temp_dict.get("current_temperature_celsius", None)
        )
        fields["live"]["wind_speed"] = self.safe_float(
            temp_dict.get("current_wind_speed_kph", None)
        )
        fields["live"]["humidity"] = self.safe_float(
            temp_dict.get("relative_humidity", None)
        )
        fields["live"]["pressure"] = self.safe_float(
            temp_dict.get("pressure_hpa", None)
        )
        fields["live"]["rain"] = self.safe_float(temp_dict.get("rain_rate_mm", None))

        fields["daily"]["max_temperature"] = self.safe_float(
            temp_dict.get("daily_max_temperature", None)
        )
        fields["daily"]["min_temperature"] = self.safe_float(
            temp_dict.get("daily_min_temperature", None)
        )
        fields["daily"]["max_wind_speed"] = self.safe_float(
            temp_dict.get("daily_max_wind_speed", None)
        )
        fields["daily"]["cumulative_rain"] = self.safe_float(
            temp_dict.get("total_daily_precipitation_at_record_timestamp", None)
        )

        return fields

    def fetch_live_data(self, station: WeatherStation) -> dict:
        """
        Fetch live weather data from a realtime.txt compatible station.

        Args:
            station (WeatherStation): The weather station object.

        Returns:
            dict: The raw live data fetched from the station.
        """
        endpoint = station.field1

        if not endpoint.endswith("/realtime.txt"):
            endpoint = f"{endpoint}/realtime.txt"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/58.0.3029.110 Safari/537.3"
        }

        response = self.make_request(endpoint, headers=headers)
        if not response or response.status_code != 200:
            raise ValueError(
                f"Error: Received status code {response.status_code if response else 'None'}"
            )

        return response.text
