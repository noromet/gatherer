from schema import WeatherRecord
from .utils import is_date_too_old, UnitConverter
import json
import requests
import datetime

field_map = {
    "temperature": "field1",
    "humidity": "field2",
    "pressure": "field4",
}

class ThingspeakReader:
    @staticmethod
    def parse(str_data: str) -> WeatherRecord:
        try:
            data = json.loads(str_data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        observation_time = datetime.datetime.strptime(data["feeds"][0]["created_at"], "%Y-%m-%dT%H:%M:%SZ")
        observation_time = observation_time.replace(tzinfo=datetime.timezone.utc) # LAS FECHAS DE THINGSPEAK SON UTC (CREO)
        
        if is_date_too_old(observation_time):
            raise ValueError("Record timestamp is too old to be stored as current.")

        return WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=observation_time,
            temperature=data["feeds"][0]["field1"],
            wind_speed=None,
            wind_direction=None,
            max_wind_speed=None,
            rain=None,
            cumulativeRain=None,
            humidity=data["feeds"][0]["field2"],
            pressure=data["feeds"][0]["field4"],
            flagged=False,
            gathererRunId=None,
            minTemp=None,
            maxTemp=None,
            windGust=None
        )
    
    @staticmethod
    def curl_endpoint(endpoint: str, station_id: str, password: str) -> str:
        endpoint = f"{endpoint}/{station_id}/feeds.json?results=1"

        response = requests.get(endpoint)
        
        # Print full URL
        print(f"Requesting {response.url}")

        return response.text
    
    @staticmethod
    def get_data(endpoint: str, params: tuple = ()) -> WeatherRecord:
        assert params[0] is not None, "station_id is null"  # station id
        
        response = ThingspeakReader.curl_endpoint(endpoint, params[0], params[2])
        parsed = ThingspeakReader.parse(response)
        return parsed