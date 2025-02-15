from schema import WeatherRecord
from .utils import safe_float
from .common import assert_date_age
import json
import requests
import datetime
from datetime import tzinfo, timezone
import logging

field_map = {
    "temperature": "field1",
    "humidity": "field2",
    "pressure": "field4",
}

class ThingspeakReader:
    @staticmethod
    def parse(str_data: str, data_timezone: tzinfo = timezone.utc, local_timezone: tzinfo = timezone.utc) -> WeatherRecord:
        try:
            data = json.loads(str_data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        observation_time = datetime.datetime.strptime(data["feeds"][0]["created_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=data_timezone)
        observation_time_utc = observation_time.astimezone(timezone.utc)
        local_observation_time = observation_time.astimezone(local_timezone)
        assert_date_age(observation_time_utc)

        return WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=local_observation_time,
            temperature=safe_float(data["feeds"][0].get("field1", None)),
            wind_speed=None,
            wind_direction=None,
            max_wind_speed=None,
            rain=None,
            cumulativeRain=None,
            humidity=safe_float(data["feeds"][0].get("field2", None)),
            pressure=safe_float(data["feeds"][0].get("field4", None)),
            flagged=False,
            gathererRunId=None,
            minTemp=None,
            maxTemp=None,
            maxWindGust=None
        )
    
    @staticmethod
    def curl_endpoint(endpoint: str, station_id: str, password: str) -> str:
        endpoint = f"{endpoint}/{station_id}/feeds.json?results=1"

        response = requests.get(endpoint)
        
        logging.info(f"Requesting {response.url}")

        return response.text
    
    @staticmethod
    def get_data(endpoint: str, params: tuple = (), station_id: str = None, data_timezone: tzinfo = timezone.utc, local_timezone: tzinfo = timezone.utc) -> WeatherRecord:
        assert params[0] is not None, "station_id is null"  # station id

        if params[1] not in (None, "NA", "na", ""):
            logging.warning("Warning: ThingspeakReader does not use api key, but it was provided.")

        if params[2] not in (None, "NA", "na", ""):
            logging.warning("Warning: ThingspeakReader does not use password, but it was provided.")
        
        response = ThingspeakReader.curl_endpoint(endpoint, params[0], params[2])
        parsed = ThingspeakReader.parse(response, data_timezone, local_timezone)
        return parsed