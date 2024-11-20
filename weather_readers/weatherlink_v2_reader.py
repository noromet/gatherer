# https://api.weatherlink.com/v2/current/{station-id}?api-key={YOUR API KEY}

from schema import WeatherRecord
from .utils import is_date_too_old, UnitConverter
import json
import requests
import datetime
import itertools

class WeatherlinkV2Reader:
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
            temperature=UnitConverter.fahrenheit_to_celsius(data["imperial"]["temp"]),
            wind_speed=UnitConverter.mph_to_kph(data["imperial"]["windSpeed"]),
            max_wind_speed=UnitConverter.mph_to_kph(data["imperial"]["windGust"]),
            wind_direction=data["winddir"],
            rain=UnitConverter.inches_to_mm(data["imperial"]["precipRate"]),
            humidity=data["humidity"],
            pressure=UnitConverter.psi_to_hpa(data["imperial"]["pressure"]),
            flagged=False,
            gathererRunId=None
        )
    
    @staticmethod
    def curl_endpoint(endpoint: str, station_id: str, api_key: str, api_secret: str) -> str:
        endpoint = endpoint.format(station_id=station_id)

        params = {
            "api-key": api_key
        }
        headers = {
            'X-Api-Secret': api_secret
        }
        response = requests.get(endpoint, params=params, headers=headers)
        
        #print full url
        print(f"Requesting {response.url}")
        
        return response.text

    
    @staticmethod
    def get_data(endpoint: str, params: tuple = ()) -> dict:
        assert params[0] is not None, "station id is null"
        assert params[1] is not None, "api key is null"
        assert params[2] is not None, "api secret is null"
        
        response = WeatherlinkV2Reader.curl_endpoint(endpoint, params[0], params[1], params[2])
        print(response)
        parsed = WeatherlinkV2Reader.parse(response)

        return parsed
