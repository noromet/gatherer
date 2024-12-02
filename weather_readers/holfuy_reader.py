from schema import WeatherRecord
from .utils import is_date_too_old, UnitConverter
import json
import requests
import datetime
import logging

class HolfuyReader:
    @staticmethod
    def parse(str_data: str, station_id: str = None) -> WeatherRecord:
        try:
            data = json.loads(str_data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        observation_time = datetime.datetime.strptime(data["dateTime"], "%Y-%m-%d %H:%M:%S")
        
        if is_date_too_old(observation_time):
            raise ValueError(f"[{station_id}]: Record timestamp is too old to be stored as current. Observation time: {observation_time}, local time: {datetime.datetime.now()}")

        return WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=observation_time,
            temperature=data["temperature"],  # Already in Celsius
            wind_speed=UnitConverter.mph_to_kph(data["wind"]["speed"]),
            wind_direction=data["wind"]["direction"],
            max_wind_speed=None,
            rain=data["rain"],  # Assuming rain is in mm
            cumulativeRain=round(data["daily"]["sum_rain"],2),  # Assuming rain is in mm
            humidity=data["humidity"],
            pressure=data["pressure"],  # Assuming pressure is in hPa
            flagged=False,
            gathererRunId=None,
            minTemp=data["daily"]["min_temp"],
            maxTemp=data["daily"]["max_temp"],
            windGust=UnitConverter.mph_to_kph(data["wind"]["gust"])
        )
    
    @staticmethod
    def curl_endpoint(endpoint: str, station_id: str, password: str) -> str:
        endpoint = f"{endpoint}?s={station_id}&pw={password}&m=JSON&tu=C&su=m/s&daily=True"

        response = requests.get(endpoint)
        
        # Print full URL
        print(f"Requesting {response.url}")
        
        return response.text

    
    @staticmethod
    def get_data(endpoint: str, params: tuple = (), station_id: str = None) -> WeatherRecord:
        assert params[0] is not None, "station_id is null"  # station id
        assert params[2] is not None, "password is null"  # password
        
        if params[1] not in (None, "NA", "na", ""):
            print("Warning: HolfuyReader does not use api key, but it was provided.")
            logging.warning(f"{[station_id]} Warning: HolfuyReader does not use api key, but it was provided.")

        response = HolfuyReader.curl_endpoint(endpoint, params[0], params[2])
        parsed = HolfuyReader.parse(response, station_id)
        return parsed