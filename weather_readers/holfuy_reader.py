from schema import WeatherRecord, WeatherStation
import json
import requests
import datetime
import logging
from datetime import timezone
from .weather_reader import WeatherReader

class HolfuyReader(WeatherReader):
    def __init__(self, live_endpoint: str, daily_endpoint: str):
        super().__init__(live_endpoint=live_endpoint, daily_endpoint=daily_endpoint)
    
    def parse(
            self,
            station: WeatherStation,
            live_data_response: str,
            daily_data_response: str) -> WeatherRecord:
        
        try:
            live_data = json.loads(live_data_response)
            historic_data = json.loads(daily_data_response)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        observation_time = datetime.datetime.strptime(live_data["dateTime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=station.data_timezone)
        observation_time_utc = observation_time.astimezone(timezone.utc)
        self.assert_date_age(observation_time_utc)
        local_observation_time = observation_time.astimezone(station.local_timezone)

        current_date = datetime.datetime.now(tz=station.data_timezone).date()
        observation_date = observation_time.date()

        if observation_time.time() >= datetime.time(0, 0) and observation_time.time() <= datetime.time(0, 15) and observation_date == current_date:
            use_daily = False
        else:
            use_daily = True

        wr = WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=local_observation_time,
            temperature=live_data.get("temperature"),
            wind_speed=live_data.get("wind", {}).get("speed"),
            max_wind_speed=None,
            wind_direction=live_data.get("wind", {}).get("direction"),
            rain=live_data.get("rain"),
            humidity=live_data.get("humidity"),
            pressure=live_data.get("pressure"),
            flagged=False,
            gatherer_thread_id=None,
            cumulative_rain=None,
            max_temperature=None,
            min_temperature=None,
            wind_gust=live_data.get("wind", {}).get("gust"),
            max_wind_gust=None
        )

        if use_daily:
            wr.min_temperature = live_data.get("daily", {}).get("min_temp")
            wr.max_temperature = live_data.get("daily", {}).get("max_temp")
            wr.cumulative_rain = round(live_data.get("daily", {}).get("sum_rain"), 2)
        else:
            logging.info(f"Discarding daily data. Observation time: {observation_time}, Local time: {datetime.datetime.now(tz=station.local_timezone)}")

        return wr
    
    
    def call_live_endpoint(self, station_id: str, password: str) -> str:
        endpoint = f"{self.live_endpoint}?s={station_id}&pw={password}&m=JSON&tu=C&su=km/h&daily=True"

        response = requests.get(endpoint)
        
        logging.info(f"Requesting {response.url}")
        
        return response.text
    
    
    def call_historic_endpoint(self, station_id: str, password: str) -> str:
        endpoint = f"{self.daily_endpoint}?s={station_id}&pw={password}&m=JSON&tu=C&su=km/h&type=2&mback=60"

        response = requests.get(endpoint)
    
        logging.info(f"Requesting {response.url}")
        
        return response.text

    
    
    def get_data(self, station: WeatherStation) -> WeatherRecord:

        live_response = self.call_live_endpoint(station.field1, station.field3)
        historical_response = self.call_historic_endpoint(station.field1, station.field3)

        parsed = self.parse(station=station, live_data_response=live_response, daily_data_response=historical_response)
        return parsed