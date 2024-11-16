#https://api.weather.com/v2/pws/observations/current?stationId=ISOTODEL6&format=json&units=e&apiKey=a952662893aa49f992662893aad9f98d

from schema import WeatherRecord
from .utils import is_date_too_old, UnitConverter
import json
import requests
import datetime

class WundergroundReader:
    @staticmethod
    def parse(str_data: str) -> WeatherRecord:
        try:
            data = json.loads(str_data)["observations"][0]
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        observation_time = datetime.datetime.strptime(data["obsTimeLocal"], "%Y-%m-%d %H:%M:%S")
        
        if is_date_too_old(observation_time):
            raise ValueError("Record timestamp is too old to be stored as current.")

        return WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=observation_time,
            temperature=data["metric"]["temp"],
            wind_speed=data["metric"]["windSpeed"],
            max_wind_speed=data["metric"]["windGust"],
            wind_direction=data["winddir"] if "winddir" in data else None,
            rain=data["metric"]["precipRate"],
            cumulativeRain=data["metric"]["precipTotal"],
            humidity=data["humidity"],
            pressure=data["metric"]["pressure"],
            flagged=False,
            gathererRunId=None
        )
    @staticmethod
    def curl_endpoint(endpoint: str, did: str, token: str) -> str:
        response = requests.get(endpoint, {
            "stationId": did,
            "apiKey": token,
            "format": "json",
            "units": "m",
            "numericPrecision": "decimal"
        })
        
        #print full url
        print(f"Requesting {response.url}")
        
        return response.text

    
    @staticmethod
    def get_data(endpoint: str, params: tuple = ()) -> dict:
        assert params[0] is not None #did
        assert params[1] is not None #apiToken
        
        if params[2] not in (None, "NA", "na", ""):
            print("Warning: WundergroundReader does not use password, but it was provided.")

        response = WundergroundReader.curl_endpoint(endpoint, params[0], params[1])
        parsed = WundergroundReader.parse(response)
        return parsed