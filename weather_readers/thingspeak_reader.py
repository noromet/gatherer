from schema import WeatherRecord, WeatherStation
from .weather_reader import WeatherReader
import json
import requests
import datetime
from datetime import timezone
import logging


class ThingspeakReader(WeatherReader):
    FIELD_MAP = {
        "temperature": "field1",
        "humidity": "field2",
        "pressure": "field4",
    }

    def __init__(self, live_endpoint: str, daily_endpoint: str = None):
        super().__init__(live_endpoint=live_endpoint, daily_endpoint=daily_endpoint)

    def parse(self,
            station: WeatherStation,
            live_data_response: str | None, 
            daily_data_response: str | None, 
        ) -> WeatherRecord:

        data = {}
        try:
            data = json.loads(live_data_response)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        observation_time = datetime.datetime.strptime(data["feeds"][0]["created_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=station.data_timezone)
        observation_time_utc = observation_time.astimezone(timezone.utc)
        local_observation_time = observation_time.astimezone(station.local_timezone)
        self.assert_date_age(observation_time_utc)

        temperature = self.safe_float(data.get("feeds")[0].get(self.FIELD_MAP.get("temperature"), None))
        humidity = self.safe_float(data.get("feeds")[0].get(self.FIELD_MAP.get("humidity"), None))
        pressure = self.safe_float(data.get("feeds")[0].get(self.FIELD_MAP.get("pressure"), None))

        wr = WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=local_observation_time,
            temperature=temperature,
            wind_speed=None,
            max_wind_speed=None,
            wind_direction=None,
            rain=None,
            humidity=humidity,
            pressure=pressure,
            flagged=False,
            gatherer_thread_id=None,
            cumulative_rain=None,
            max_temperature=None,
            min_temperature=None,
            wind_gust=None,
            max_wind_gust=None
        )

        return wr
    
    
    def call_endpoint(self, station_id) -> str:
        endpoint = f"{self.live_endpoint}/{station_id}/feeds.json?results=1"

        response = requests.get(endpoint)
        
        logging.info(f"Requesting {response.url}")

        return response.text
    
    
    def get_data(self, station: WeatherStation) -> WeatherRecord:
        if not station.field1:
            raise ValueError("Missing connection parameter.")
        
        response = self.call_endpoint(station_id=station.field1)
        return self.parse(station=station, live_data_response=response, daily_data_response=None)