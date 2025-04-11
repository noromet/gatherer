"""
RealtimeReader
This class is responsible for reading weather data from a real-time weather station.
It fetches data from a specified endpoint and parses it into a WeatherRecord object.
"""

from datetime import timezone, datetime
from schema import WeatherRecord, WeatherStation
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

    def __init__(self):
        super().__init__()
        self.required_fields = ["field1"]

    def parse(self, station: WeatherStation, data: dict) -> WeatherRecord:
        valid_keys = self.INDEX_TO_DATA.keys()
        for index, item in enumerate(data["live"].strip().split(" ")):
            item = item.strip()
            if not item:
                continue
            if index in valid_keys:
                data[self.INDEX_TO_DATA[index]] = item
            index += 1

        _time = datetime.strptime(data["time"], "%H:%M:%S")
        _date = self.smart_parse_date(data["date"], timezone=station.data_timezone)

        if _time is None or _date is None:
            raise ValueError("Cannot accept a reading without a timestamp.")

        timestamp = datetime.combine(_date, _time.time(), tzinfo=station.data_timezone)

        observation_time_utc = timestamp.astimezone(timezone.utc)
        self.assert_date_age(observation_time_utc)

        local_observation_time = observation_time_utc.astimezone(station.local_timezone)

        wind_direction = float(data.get("current_wind_direction", None))
        temperature = float(data.get("current_temperature_celsius", None))
        wind_speed = float(data.get("current_wind_speed_kph", None))
        max_wind_speed = float(data.get("daily_max_wind_speed", None))
        cumulative_rain = float(
            data.get("total_daily_precipitation_at_record_timestamp", None)
        )
        humidity = float(data.get("relative_humidity", None))
        pressure = float(data.get("pressure_hpa", None))
        max_temperature = float(data.get("daily_max_temperature", None))
        min_temperature = float(data.get("daily_min_temperature", None))
        rain = float(data.get("rain_rate_mm", None))

        wr = WeatherRecord(
            wr_id=None,
            station_id=station.id,
            source_timestamp=local_observation_time,
            temperature=temperature,
            wind_speed=wind_speed,
            max_wind_speed=max_wind_speed,
            wind_direction=wind_direction,
            rain=rain,
            humidity=humidity,
            pressure=pressure,
            flagged=False,
            gatherer_thread_id=None,
            cumulative_rain=cumulative_rain,
            max_temperature=max_temperature,
            min_temperature=min_temperature,
            wind_gust=None,
            max_wind_gust=None,
        )

        return wr

    def fetch_data(self, station: WeatherStation) -> dict:
        endpoint = station.field1

        if not endpoint.endswith("/realtime.txt"):
            endpoint = f"{endpoint}/realtime.txt"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/58.0.3029.110 Safari/537.3"
        }

        response = self.make_request(endpoint, headers=headers)
        if response.status_code != 200:
            raise ValueError(f"Error: Received status code {response.status_code}")

        return {"live": response.text}
