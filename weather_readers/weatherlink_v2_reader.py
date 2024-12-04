# https://api.weatherlink.com/v2/current/{station-id}?api-key={YOUR API KEY}

from schema import WeatherRecord
from .utils import is_date_too_old, UnitConverter
import json
import requests
import datetime
import logging

def read_sensor_data(sensors: list, station_id: str = None) -> dict:
    possible_data_values = {

    }

    for sensor in sensors:
        ...

class WeatherlinkV2Reader:
    @staticmethod
    def parse(current_str_data: str, historic_str_data: str, station_id: str = None) -> WeatherRecord:
        try:
            current_data = json.loads(current_str_data)
            current_data = current_data["sensors"][0]["data"][0]

            if historic_str_data is not None:
                historic_data = json.loads(historic_str_data)
                historic_data 
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        observation_time = datetime.datetime.fromtimestamp(current_data["ts"], tz=datetime.timezone(datetime.timedelta(seconds=current_data["tz_offset"])))

        print(f"Observation time: {observation_time}")

        if is_date_too_old(observation_time):
            raise ValueError("Record timestamp is too old to be stored as current.")

        wr = WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=observation_time,
            temperature=current_data["temp_out"],
            wind_speed=UnitConverter.mph_to_kph(float(current_data["wind_speed"])),
            wind_direction=current_data["wind_dir"],
            max_wind_speed=None,
            rain=current_data["rain_rate_mm"],
            cumulativeRain=None,
            humidity=current_data["hum_out"],
            pressure=None,
            flagged=False,
            gathererRunId=None,
            maxTemp=None,
            minTemp=None,
            maxWindGust=None
        )

        obstime_local_tz = observation_time.astimezone(datetime.datetime.now().astimezone().tzinfo)

        if not (observation_time.hour == 0 and observation_time.minute < 15) \
            and obstime_local_tz.date() == datetime.datetime.now().date() \
            and historic_str_data is not None:
            ...

    @staticmethod
    def curl_current_endpoint(endpoint: str, station_id: str, api_key: str, api_secret: str) -> str:
        endpoint = endpoint.format(mode="current", station_id=station_id)

        params = {
            "api-key": api_key,
            "t": int(datetime.datetime.now().timestamp())
        }
        headers = {
            'X-Api-Secret': api_secret
        }
        response = requests.get(endpoint, params=params, headers=headers)
        
        #print full url
        print(f"Requesting {response.url}")
        
        return response.text

    @staticmethod
    def curl_historic_endpoint(endpoint: str, station_id: str, api_key: str, api_secret: str) -> str:
        endpoint = endpoint.format(mode="historic", station_id=station_id)

        #start timestamp is today at 00:00:00, end timestamp is today at 23:59:59
        params = {
            "api-key": api_key,
            "t": int(datetime.datetime.now().timestamp()),
            "start-timestamp": int(datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()),
            "end-timestamp": int(datetime.datetime.now().replace(hour=23, minute=59, second=59, microsecond=0).timestamp())
        }
        headers = {
            'X-Api-Secret': api_secret
        }
        response = requests.get(endpoint, params=params, headers=headers)
        
        #print full url
        print(f"Requesting {response.url}")

        if response.status_code != 200:
            logging.warning(f"Request failed with status code {response.status_code}. Is the subscription active?")
            return None
        else:
            return response.text
    
    @staticmethod
    def get_data(endpoint: str, params: tuple = (), station_id: str = None) -> dict:
        assert params[0] is not None, "station id is null"
        assert params[1] is not None, "api key is null"
        assert params[2] is not None, "api secret is null"
        
        current_response = WeatherlinkV2Reader.curl_current_endpoint(endpoint, params[0], params[1], params[2])
        # historic_response = WeatherlinkV2Reader.curl_historic_endpoint(endpoint, params[0], params[1], params[2])
        historic_response = None
        parsed = WeatherlinkV2Reader.parse(current_response, historic_response, station_id=station_id)

        return parsed
