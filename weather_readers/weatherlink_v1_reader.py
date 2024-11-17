from schema import WeatherRecord
from .utils import is_date_too_old, UnitConverter
import json
import requests
import datetime

# https://api.weather.com/v2/pws/observations/current?stationId=ISOTOYAM2&apiKey=317bd2820daf46edbbd2820daf26ede4&format=json&units=s&numericPrecision=decimal

class WeatherLinkV1Reader:
    @staticmethod
    def parse(str_data: str) -> WeatherRecord:
        try:
            data = json.loads(str_data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        observation_time = datetime.datetime.strptime(data["observation_time_rfc822"], "%a, %d %b %Y %H:%M:%S %z")
        
        if is_date_too_old(observation_time):
            raise ValueError("Record timestamp is too old to be stored as current.")
        
        if "temp_c" in data:
            temperature = data["temp_c"]
        else:
            temperature = UnitConverter.fahrenheit_to_celsius(float(data["davis_current_observation"]["temp_in_f"]))
        
        return WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=observation_time,
            temperature=temperature,
            wind_speed=UnitConverter.mph_to_kph(float(data["wind_mph"])),
            wind_direction=data["wind_degrees"],
            max_wind_speed=UnitConverter.mph_to_kph(float(data["davis_current_observation"]["wind_day_high_mph"])), #daily
            rain=data["davis_current_observation"]["rain_rate_in_per_hr"],
            cumulativeRain=data["davis_current_observation"]["rain_day_in"],
            humidity=data["relative_humidity"],
            pressure=data["pressure_mb"], #mb = hpa
            flagged=False,
            gathererRunId=None,
            maxTemp=UnitConverter.fahrenheit_to_celsius(float(data["davis_current_observation"]["temp_day_high_f"])),
            minTemp=UnitConverter.fahrenheit_to_celsius(float(data["davis_current_observation"]["temp_day_low_f"])),
            windGust=UnitConverter.mph_to_kph(float(data["davis_current_observation"]["wind_ten_min_avg_mph"]))
        )
    
    @staticmethod
    def curl_endpoint(endpoint: str, user_did: str, password: str, apiToken: str) -> str:
        response = requests.get(endpoint, {
            "user": user_did,
            "pass": password,
            "apiToken": apiToken
        })
        
        #print full url
        print(f"Requesting {response.url}")
        
        return response.text

    
    @staticmethod
    def get_data(endpoint: str, params: tuple = ()) -> dict:
        assert params[0] is not None
        assert params[1] is not None
        assert params[2] is not None
        
        response = WeatherLinkV1Reader.curl_endpoint(endpoint, params[0], params[2], params[1])#did, password, apiToken are field1, field3, field2
        parsed = WeatherLinkV1Reader.parse(response)
        return parsed