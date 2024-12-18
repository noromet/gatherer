from schema import WeatherRecord
from .utils import is_date_too_old, UnitConverter, safe_float
import json
import requests
import datetime
import logging

class EcowittReader:
    @staticmethod
    def parse(live_str_data: str, daily_str_data: str, station_id: str = None) -> WeatherRecord:
        try:
            live_data = json.loads(live_str_data)
            daily_data = json.loads(daily_str_data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        # observation_time = datetime.datetime.strptime(data["dateTime"], "%Y-%m-%d %H:%M:%S")
        
        # if is_date_too_old(observation_time):
        #     raise ValueError(f"[{station_id}]: Record timestamp is too old to be stored as current. Observation time: {observation_time}, local time: {datetime.datetime.now()}")

        # return WeatherRecord(
        #     id=None,
        #     station_id=None,
        #     source_timestamp=observation_time,
        #     temperature=data["temperature"],  # Already in Celsius
        #     wind_speed=UnitConverter.mph_to_kph(data["wind"]["speed"]),
        #     wind_direction=data["wind"]["direction"],
        #     max_wind_speed=None,
        #     rain=data["rain"],  # Assuming rain is in mm
        #     cumulativeRain=round(data["daily"]["sum_rain"],2),  # Assuming rain is in mm
        #     humidity=data["humidity"],
        #     pressure=data["pressure"],  # Assuming pressure is in hPa
        #     flagged=False,
        #     gathererRunId=None,
        #     minTemp=data["daily"]["min_temp"],
        #     maxTemp=data["daily"]["max_temp"],
        #     maxWindGust=UnitConverter.mph_to_kph(data["wind"]["gust"])
        # )
    
    @staticmethod
    def curl_live_endpoint(endpoint: str, mac: str, api_key: str, application_key: str) -> str:
        url = f"{endpoint}?mac={mac}&api_key={api_key}&application_key={application_key}"
        url += "&temp_unitid=1&pressure_unitid=3&wind_speed_unitid=7&rainfall_unitid=12"
    
        response = requests.get(url)
        
        # Print full URL
        print(f"Requesting {response.url}")
        
        return response.text
    
    @staticmethod
    def curl_daily_endpoint(daily_endpoint: str, mac: str, api_key: str, application_key: str) -> str:
        start_date = datetime.datetime.now().strftime("%Y-%m-%d 00:00:00")
        end_date = datetime.datetime.now().strftime("%Y-%m-%d 23:59:59")
        
        url = f"{daily_endpoint}?mac={mac}&api_key={api_key}&application_key={application_key}"
        url += "&temp_unitid=1&pressure_unitid=3&wind_speed_unitid=7&rainfall_unitid=12"
        url += f"&cycle_type=auto&start_date={start_date}&end_date={end_date}"
        url += "&call_back=outdoor.temperature,outdoor.humidity"

        response = requests.get(url)
        
        # Print full URL
        print(f"Requesting {response.url}")
        
        return response.text

    
    @staticmethod
    def get_data(live_endpoint: str, daily_endpoint: str, params: tuple = (), station_id: str = None) -> WeatherRecord:
        assert params[0] is not None, "station_id is null"  # station id
        assert params[1] is not None, "api_key is null"  # api key
        assert params[2] is not None, "application_key is null"  # application key
        
        live_response = EcowittReader.curl_live_endpoint(live_endpoint, params[0], params[1], params[2])
        daily_response = EcowittReader.curl_daily_endpoint(daily_endpoint, params[0], params[1], params[2])
        
        parsed = EcowittReader.parse(live_response, daily_response, station_id)

        return parsed