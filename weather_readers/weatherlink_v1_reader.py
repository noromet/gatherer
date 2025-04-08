from schema import WeatherRecord, WeatherStation
from .utils import UnitConverter
from .weather_reader import WeatherReader
import json
import requests
import datetime
import logging
from datetime import timezone

# https://api.weather.com/v2/pws/observations/current?stationId=ISOTOYAM2&apiKey=317bd2820daf46edbbd2820daf26ede4&format=json&units=s&numericPrecision=decimal

class WeatherLinkV1Reader(WeatherReader):
    def __init__(self, live_endpoint: str):
        self.live_endpoint=live_endpoint
        self.required_fields = ["field1", "field2", "field3"]
        
    def parse(self, station: WeatherStation, data: dict) -> WeatherRecord:
        try:
            live_data = json.loads(data["live"])
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Invalid JSON data: {e}. Check station connection parameters."
            )
        
        observation_time = datetime.datetime.strptime(live_data["observation_time_rfc822"], "%a, %d %b %Y %H:%M:%S %z").replace(tzinfo=station.data_timezone)
        observation_time_utc = observation_time.astimezone(timezone.utc)
        self.assert_date_age(observation_time_utc)
        local_observation_time = observation_time.astimezone(station.local_timezone)
    
        current_date = datetime.datetime.now(tz=station.data_timezone).date()
        observation_date = observation_time.date()
        if observation_time.time() >= datetime.time(0, 0) and observation_time.time() <= datetime.time(0, 15) and observation_date == current_date:
            use_daily = False
        else:
            use_daily = True

        temperature = self.safe_float(live_data.get("temp_c"))
        wind_speed = UnitConverter.mph_to_kph(self.safe_float(live_data.get("wind_mph")))
        wind_direction = self.safe_float(live_data.get("wind_degrees"))
        rain = UnitConverter.inches_to_mm(self.safe_float(live_data.get("davis_current_observation").get("rain_rate_in_per_hr")))
        humidity = self.safe_float(live_data.get("relative_humidity"))
        pressure = self.safe_float(live_data.get("pressure_mb"))
        wind_gust = UnitConverter.mph_to_kph(
            self.safe_float(live_data["davis_current_observation"].get("wind_ten_min_gust_mph"))
        )

        max_wind_speed = UnitConverter.mph_to_kph(
            self.safe_float(live_data["davis_current_observation"].get("wind_day_high_mph"))
        )
        max_temperature = UnitConverter.fahrenheit_to_celsius(
            self.safe_float(live_data["davis_current_observation"].get("temp_day_high_f"))
        )
        min_temperature = UnitConverter.fahrenheit_to_celsius(
            self.safe_float(live_data["davis_current_observation"].get("temp_day_low_f"))
        )
        cumulative_rain = UnitConverter.inches_to_mm(
            self.safe_float(live_data["davis_current_observation"].get("rain_day_in"))
        )

        wr = WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=local_observation_time,
            temperature=temperature,
            wind_speed=wind_speed,
            max_wind_speed=None,
            wind_direction=wind_direction,
            rain=rain,
            humidity=humidity,
            pressure=pressure,
            flagged=False,
            gatherer_thread_id=None,
            cumulative_rain=None,
            max_temperature=None,
            min_temperature=None,
            wind_gust=wind_gust,
            max_wind_gust=None
        )
        
        if use_daily:
            wr.max_temperature = max_temperature
            wr.min_temperature = min_temperature
            wr.max_wind_speed = max_wind_speed
            wr.cumulative_rain = cumulative_rain
        else:
            logging.warning(f"Discarding daily data. Observation time: {observation_time}, Local time: {datetime.datetime.now(tz=station.local_timezone)}")

        return wr
    
    
    def fetch_data(self, station: WeatherStation) -> dict:
        user, apiToken, password = station.field1, station.field2, station.field3

        response = requests.get(self.live_endpoint, {
            "user": user,
            "pass": password,
            "apiToken": apiToken
        })
        logging.info(f"Requesting {response.url}")

        if response.status_code != 200:
            logging.error(f"Request failed with status code {response.status_code}. Check station connection parameters.")
            return None
        
        return {"live": response.text}
