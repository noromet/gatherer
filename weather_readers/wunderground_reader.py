#https://api.weather.com/v2/pws/observations/current?stationId=ISOTODEL6&format=json&units=e&apiKey=a952662893aa49f992662893aad9f98d

from schema import WeatherRecord, WeatherStation
import json
import requests
import datetime
import logging
from datetime import tzinfo, timezone
from .weather_reader import WeatherReader

class WundergroundReader(WeatherReader):
    def __init__(self, live_endpoint: str, daily_endpoint: str):
        super().__init__(live_endpoint=live_endpoint, daily_endpoint=daily_endpoint)

    def parse(
            self,
            station: WeatherStation,
            live_data_response: str,
            daily_data_response: str
    ) -> WeatherRecord:
        try:
            live_data = json.loads(live_data_response)["observations"][0]
            daily_data = json.loads(daily_data_response)["summaries"][-1]

            assert live_data["stationID"] == daily_data["stationID"], "Something broke: live and daily data are not from the same station."
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        # datetime management
        observation_time = datetime.datetime.strptime(live_data["obsTimeLocal"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=station.data_timezone)
        observation_time_utc = datetime.datetime.strptime(live_data["obsTimeUtc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        self.assert_date_age(observation_time_utc)
        
        local_observation_time = observation_time.astimezone(station.local_timezone)
        current_date = datetime.datetime.now(tz=station.data_timezone).date()
        observation_date = observation_time.date()
        if observation_time.time() >= datetime.time(0, 0) and observation_time.time() <= datetime.time(0, 15) and observation_date == current_date:
            use_daily = False
        else:
            use_daily = True

        live_metric_data = live_data.get("metric") #esquizo
        if live_metric_data is None:
            raise ValueError("No metric data found in live data.")
        
        temperature = live_metric_data.get("temp", None)
        wind_speed = live_metric_data.get("windSpeed", None)
        wind_direction = live_data.get("winddir", None)
        rain = live_metric_data.get("precipRate", None)
        cumulative_rain = live_metric_data.get("precipTotal", None)
        humidity = live_data.get("humidity", None)
        pressure = live_metric_data.get("pressure", None)
        wind_gust = live_metric_data.get("windGust", None)

        daily_metric_data = daily_data.get("metric")
        if use_daily:
            max_wind_speed = daily_metric_data.get("windspeedHigh", None)
            max_temperature = daily_metric_data.get("tempHigh", None)
            min_temperature = daily_metric_data.get("tempLow", None)
            max_wind_gust = daily_metric_data.get("windgustHigh", None)

        else:
            logging.warning(f"Discarding daily data. Observation time: {observation_time}, Local time: {datetime.datetime.now(tz=station.local_timezone)}")
            max_wind_speed, max_temperature, min_temperature, max_wind_gust = None, None, None, None

        wr = WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=local_observation_time,
            temperature=temperature,
            wind_speed=wind_speed,
            max_wind_speed=max_wind_speed,
            wind_direction=wind_direction,
            rain=rain,
            humidity=humidity,
            pressure=pressure,
            flagged=False,
            gatherer_thread_id=None,
            cumulative_rain=cumulative_rain,
            max_temperature=max_temperature,
            min_temperature=min_temperature,
            wind_gust=wind_gust,
            max_wind_gust=max_wind_gust
        )

        return wr
    
    
    def call_live_endpoint(self, did: str, token: str) -> str:
        response = requests.get(self.live_endpoint, {
            "stationId": did,
            "apiKey": token,
            "format": "json",
            "units": "m",
            "numericPrecision": "decimal"
        })
        
        logging.info(f"Requesting {response.url}")
        
        return response.text
    
    def call_daily_endpoint(self, did: str, token: str) -> str:
        response = requests.get(self.daily_endpoint, {
            "stationId": did,
            "apiKey": token,
            "format": "json",
            "units": "m",
            "numericPrecision": "decimal"
        })
        
        logging.info(f"Requesting {response.url}")
        
        return response.text
    
    
    def get_data(self, station: WeatherStation) -> dict:
        for value in [station.field1, station.field2]:
            if not value:
                raise ValueError(f"Missing connection parameter.")
            
        live_response = self.call_live_endpoint(did=station.field1, token=station.field2)
        daily_response = self.call_daily_endpoint(did=station.field1, token=station.field2)

        parsed = self.parse(station=station, live_data_response=live_response, daily_data_response=daily_response)
        return parsed