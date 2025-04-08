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

    def __init__(self, live_endpoint: str):
        self.live_endpoint=live_endpoint

    def parse(self, station: WeatherStation, data: dict) -> WeatherRecord:
        try:
            live_data = json.loads(data["live"])
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Invalid JSON data: {e}. Check station connection parameters."
            )
        
        observation_time = datetime.datetime.strptime(live_data["feeds"][0]["created_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=station.data_timezone)
        observation_time_utc = observation_time.astimezone(timezone.utc)
        local_observation_time = observation_time.astimezone(station.local_timezone)
        self.assert_date_age(observation_time_utc)

        temperature = self.safe_float(live_data.get("feeds")[0].get(self.FIELD_MAP.get("temperature"), None))
        humidity = self.safe_float(live_data.get("feeds")[0].get(self.FIELD_MAP.get("humidity"), None))
        pressure = self.safe_float(live_data.get("feeds")[0].get(self.FIELD_MAP.get("pressure"), None))

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
    
    
    def fetch_data(self, station: WeatherStation) -> dict:
        endpoint = f"{self.live_endpoint}/{station.id}/feeds.json?results=1"
        
        logging.info(f"Requesting {response.url}")
        response = requests.get(endpoint)

        if response.status_code != 200:
            logging.error(f"Request failed with status code {response.status_code}. Check station connection parameters.")
            return None
        
        return {"live": response.text}
    