from schema import WeatherRecord
from .utils import smart_parse_date
from .common import assert_date_age
import requests
import logging
import json
from datetime import tzinfo, timezone, datetime

class RealtimeReader:        
    @staticmethod
    def parse(str_data: str, station_id: str = None, data_timezone: tzinfo = timezone.utc, local_timezone: tzinfo = timezone.utc) -> WeatherRecord:
        data = {}
        valid_keys = INDEX_TO_DATA.keys()
        for index, item in enumerate(str_data.strip().split(" ")):
            item = item.strip()
            if not item:
                continue
            if index in valid_keys:
                data[INDEX_TO_DATA[index]] = item
            index += 1    

        _time = datetime.strptime(data["time"], "%H:%M:%S")
        _date = smart_parse_date(data["date"], timezone=data_timezone)

        print(
            json.dumps(
                data, indent=4
            )
        )

        if _time is None or _date is None:
            raise ValueError("Cannot accept a reading without a timestamp.")
        
        timestamp = datetime.combine(_date, _time.time(), tzinfo=data_timezone)

        observation_time_utc = timestamp.astimezone(timezone.utc)
        assert_date_age(observation_time_utc)

        local_observation_time = observation_time_utc.astimezone(local_timezone)
            
        wind_direction = float(data.get("current_wind_direction", None))
        temperature = float(data.get("current_temperature_celsius", None))
        wind_speed = float(data.get("current_wind_speed_kph", None))
        max_wind_speed = float(data.get("daily_max_wind_speed", None))
        cumulativeRain = float(data.get("total_daily_precipitation_at_record_timestamp", None))
        humidity = float(data.get("relative_humidity", None))
        pressure = float(data.get("pressure_hpa", None))
        maxTemp = float(data.get("daily_max_temperature", None))
        minTemp = float(data.get("daily_min_temperature", None))
        rain = float(data.get("rain_rate_mm", None))

        wr = WeatherRecord(
            id=None,
            station_id=station_id,
            source_timestamp=local_observation_time,
            temperature=temperature,
            wind_speed=wind_speed,
            max_wind_speed=max_wind_speed,
            wind_direction=wind_direction,
            rain=rain,
            cumulativeRain=cumulativeRain,
            humidity=humidity,
            pressure=pressure,
            flagged=False,
            gathererRunId=None,
            maxTemp=maxTemp,
            minTemp=minTemp,
            maxWindGust=max_wind_speed
        )

        return wr
    
    @staticmethod
    def curl_endpoint(endpoint: str) -> str:
        if not endpoint.endswith("/realtime.txt"):
            endpoint = f"{endpoint}/realtime.txt"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

        response = requests.get(endpoint, headers=headers, timeout=5)
        logging.info(f"Requesting {response.url}")
        if response.status_code != 200:
            raise Exception(f"Error: Received status code {response.status_code}")
        return response.text
    
    @staticmethod
    def get_data(endpoint: str, station_id: str = None, data_timezone: tzinfo = timezone.utc, local_timezone: tzinfo = timezone.utc) -> dict:
        raw_data = RealtimeReader.curl_endpoint(endpoint)
        return RealtimeReader.parse(raw_data, station_id=station_id, data_timezone=data_timezone, local_timezone=local_timezone)

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
    32: "daily_max_wind_speed"
}