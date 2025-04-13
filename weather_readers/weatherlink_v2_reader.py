"""
This module defines the `WeatherlinkV2Reader` class for fetching and parsing weather data
from the WeatherLink V2 API. It processes live and historic weather data into a standardized
`WeatherRecord` format.
"""

import datetime
import logging
from schema import WeatherRecord, WeatherStation
from .weather_reader import WeatherReader
from .utils import UnitConverter


class WeatherlinkV2Reader(WeatherReader):
    """
    A weather data reader for the WeatherLink V2 API.

    This class fetches live and historic weather data from the WeatherLink V2 API
    and parses it into a `WeatherRecord` object. It handles data aggregation and
    transformation for various weather parameters.
    """

    def __init__(self, live_endpoint: str, daily_endpoint: str):
        super().__init__()
        self.live_endpoint = live_endpoint
        self.daily_endpoint = daily_endpoint
        self.required_fields = ["field1", "field2", "field3"]

    def handle_current_data(self, current: list) -> dict:
        """
        Process and aggregate current weather data.

        Args:
            current (list): List of sensor data.

        Returns:
            dict: Aggregated weather data.
        """
        live_response_keys = {
            "timestamp": {"ts": []},
            "temperature": {"temp": [], "temp_out": []},
            "wind_speed": {"wind_speed": [], "wind_speed_last": []},
            "wind_gust": {
                "wind_speed_hi_last_10_min": [],
                "wind_gust": [],
                "wind_gust_10_min": [],
            },
            "wind_direction": {"wind_dir": [], "wind_dir_last": []},
            "rain": {
                "rain_rate_mm": [],
                "rain_rate_last_mm": [],
            },
            "cumulative_rain": {"rain_day_mm": [], "rainfall_daily_mm": []},
            "humidity": {"hum": [], "hum_out": []},
            "pressure": {
                "bar": [],
                "bar_sea_level": [],
            },
        }

        for sensor in current:
            for data_point in sensor.get("data", []):
                for keyset in live_response_keys.values():
                    for key in keyset:
                        if key in data_point:
                            keyset[key].append(data_point[key])

        timestamp = self.max_or_none(live_response_keys["timestamp"]["ts"])

        temperature = self.coalesce(
            [
                self.coalesce(live_response_keys["temperature"]["temp"]),
                self.coalesce(live_response_keys["temperature"]["temp_out"]),
            ]
        )
        wind_speed = self.coalesce(
            [
                self.coalesce(live_response_keys["wind_speed"]["wind_speed"]),
                self.max_or_none(live_response_keys["wind_speed"]["wind_speed_last"]),
            ]
        )
        wind_direction = self.coalesce(
            [
                self.coalesce(live_response_keys["wind_direction"]["wind_dir"]),
                self.max_or_none(live_response_keys["wind_direction"]["wind_dir_last"]),
            ]
        )
        wind_gust = self.coalesce(
            [
                self.max_or_none(
                    live_response_keys["wind_gust"]["wind_speed_hi_last_10_min"]
                ),
                self.coalesce(live_response_keys["wind_gust"]["wind_gust"]),
            ]
        )
        rain = self.coalesce(
            [
                self.coalesce(live_response_keys["rain"]["rain_rate_mm"]),
                self.coalesce(live_response_keys["rain"]["rain_rate_last_mm"]),
            ]
        )
        cumulative_rain = self.coalesce(
            [
                self.max_or_none(live_response_keys["cumulative_rain"]["rain_day_mm"]),
                self.max_or_none(
                    live_response_keys["cumulative_rain"]["rainfall_daily_mm"]
                ),
            ]
        )
        humidity = self.coalesce(
            [
                self.coalesce(live_response_keys["humidity"]["hum"]),
                self.coalesce(live_response_keys["humidity"]["hum_out"]),
            ]
        )
        pressure = self.coalesce(
            [
                self.coalesce(live_response_keys["pressure"]["bar"]),
                self.coalesce(live_response_keys["pressure"]["bar_sea_level"]),
            ]
        )

        return (
            timestamp,
            temperature,
            wind_speed,
            wind_direction,
            rain,
            cumulative_rain,
            humidity,
            pressure,
            wind_gust,
        )

    def handle_historic_data(self, historic: list) -> dict:
        """
        Process and aggregate historic weather data.

        Args:
            historic (list): List of historic sensor data.

        Returns:
            dict: Aggregated historic weather data.
        """
        historical_response_keys = {
            "max_wind_speed": {
                "wind_speed_hi": [],
            },
            "cumulative_rain": {
                "rainfall_mm": [],
            },
            "max_temp": {"temp_hi": []},
            "min_temp": {"temp_lo": []},
        }

        for sensor in historic:
            for data_point in sensor.get("data", []):
                for keyset in historical_response_keys.values():
                    for key in keyset:
                        if key in data_point:
                            keyset[key].append(data_point[key])

        max_wind_speed = self.max_or_none(
            historical_response_keys["max_wind_speed"]["wind_speed_hi"]
        )
        cumulative_rain = self.max_or_none(
            historical_response_keys["cumulative_rain"]["rainfall_mm"]
        )
        max_temp = self.max_or_none(historical_response_keys["max_temp"]["temp_hi"])
        min_temp = self.min_or_none(historical_response_keys["min_temp"]["temp_lo"])

        return max_wind_speed, cumulative_rain, max_temp, min_temp

    def parse(self, station: WeatherStation, data: dict) -> WeatherRecord:
        """
        Parse the fetched data into a WeatherRecord object.

        Args:
            station (WeatherStation): The weather station object.
            data (dict): The raw data fetched from the API.

        Returns:
            WeatherRecord: The parsed weather record.
        """

        current_data = data["live"]
        current_data = current_data.get("sensors", None)

        daily_data = None
        if data["daily"]:
            daily_data = data["daily"]
            daily_data = daily_data.get("sensors", None)

        (
            timestamp,
            temperature,
            wind_speed,
            wind_direction,
            rain,
            cumulative_rain,
            humidity,
            pressure,
            wind_gust,
        ) = self.handle_current_data(current_data)

        if daily_data is not None:
            max_wind_speed, cumulative_rain_historic, max_temp, min_temp = (
                self.handle_historic_data(daily_data)
            )
        else:
            max_wind_speed, cumulative_rain_historic, max_temp, min_temp = (
                None,
                None,
                None,
                None,
            )

        final_cumulative_rain = self.coalesce(
            [cumulative_rain, cumulative_rain_historic]
        )

        local_observation_time = datetime.datetime.fromtimestamp(
            timestamp, tz=station.data_timezone
        ).astimezone(station.local_timezone)

        fields = self.get_fields()

        fields["source_timestamp"] = local_observation_time

        fields["instant"]["temperature"] = UnitConverter.fahrenheit_to_celsius(
            temperature
        )
        fields["instant"]["wind_speed"] = UnitConverter.mph_to_kph(wind_speed)
        fields["instant"]["wind_direction"] = wind_direction
        fields["instant"]["rain"] = rain
        fields["instant"]["humidity"] = humidity
        fields["instant"]["pressure"] = UnitConverter.psi_to_hpa(pressure)
        fields["instant"]["wind_gust"] = UnitConverter.mph_to_kph(wind_gust)

        fields["daily"]["max_wind_speed"] = UnitConverter.mph_to_kph(max_wind_speed)
        fields["daily"]["cumulative_rain"] = final_cumulative_rain
        fields["daily"]["max_temperature"] = UnitConverter.fahrenheit_to_celsius(
            max_temp
        )
        fields["daily"]["min_temperature"] = UnitConverter.fahrenheit_to_celsius(
            min_temp
        )

        return fields

    def fetch_data(self, station: WeatherStation) -> dict:
        """
        Fetch live and historic weather data from the WeatherLink V2 API.

        Args:
            station (WeatherStation): The weather station object.

        Returns:
            dict: A dictionary containing live and historic weather data.
        """
        station_id, api_key, api_secret = station.field1, station.field2, station.field3

        live_url = self.live_endpoint.format(mode="current", station_id=station_id)
        params = {"api-key": api_key, "t": int(datetime.datetime.now().timestamp())}
        headers = {"X-Api-Secret": api_secret}
        live_response = self.make_request(live_url, params=params, headers=headers)

        if live_response.status_code != 200:
            logging.error(
                "Request failed with status code %d. Check station connection parameters.",
                live_response.status_code,
            )
            return None

        daily_url = self.daily_endpoint.format(mode="historic", station_id=station_id)
        _15_minutes_ago = datetime.datetime.now() - datetime.timedelta(minutes=15)
        params = {
            "api-key": api_key,
            "t": int(datetime.datetime.now().timestamp()),
            "start-timestamp": int(_15_minutes_ago.timestamp()),
            "end-timestamp": int(
                datetime.datetime.now()
                .replace(hour=23, minute=59, second=59, microsecond=0)
                .timestamp()
            ),
        }
        daily_response = self.make_request(daily_url, params=params, headers=headers)

        if daily_response.status_code != 200:
            logging.warning(
                "Request failed with status code %d. Is the subscription active?",
                daily_response.status_code,
            )
            daily_response = None

        ret_dict = {"live": live_response.json()}

        if daily_response is not None:
            ret_dict["daily"] = daily_response.json()

        return ret_dict
