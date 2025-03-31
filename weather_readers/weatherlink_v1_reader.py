from schema import WeatherRecord
from .utils import UnitConverter, safe_float
from .common import assert_date_age
import json
import requests
import datetime
import logging
from datetime import tzinfo, timezone

# https://api.weather.com/v2/pws/observations/current?stationId=ISOTOYAM2&apiKey=317bd2820daf46edbbd2820daf26ede4&format=json&units=s&numericPrecision=decimal

class WeatherLinkV1Reader:
    @staticmethod
    def parse(str_data: str, data_timezone: tzinfo = timezone.utc, local_timezone: tzinfo = timezone.utc) -> WeatherRecord:
        try:
            data = json.loads(str_data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        observation_time = datetime.datetime.strptime(data["observation_time_rfc822"], "%a, %d %b %Y %H:%M:%S %z").replace(tzinfo=data_timezone)
        observation_time_utc = observation_time.astimezone(timezone.utc)
        assert_date_age(observation_time_utc)
        local_observation_time = observation_time.astimezone(local_timezone)
    
        current_date = datetime.datetime.now(tz=data_timezone).date()
        observation_date = observation_time.date()
        if observation_time.time() >= datetime.time(0, 0) and observation_time.time() <= datetime.time(0, 15) and observation_date == current_date:
            use_daily = False
        else:
            use_daily = True

        temperature = safe_float(data.get("temp_c"))
        wind_speed = UnitConverter.mph_to_kph(safe_float(data.get("wind_mph")))
        wind_direction = safe_float(data.get("wind_degrees"))
        rain = UnitConverter.inches_to_mm(safe_float(data.get("davis_current_observation").get("rain_rate_in_per_hr")))
        humidity = safe_float(data.get("relative_humidity"))
        pressure = safe_float(data.get("pressure_mb"))
        wind_gust = UnitConverter.mph_to_kph(
            safe_float(data["davis_current_observation"].get("wind_ten_min_gust_mph"))
        )

        max_wind_speed = UnitConverter.mph_to_kph(
            safe_float(data["davis_current_observation"].get("wind_day_high_mph"))
        )
        max_temperature = UnitConverter.fahrenheit_to_celsius(
            safe_float(data["davis_current_observation"].get("temp_day_high_f"))
        )
        min_temperature = UnitConverter.fahrenheit_to_celsius(
            safe_float(data["davis_current_observation"].get("temp_day_low_f"))
        )
        cumulative_rain = UnitConverter.inches_to_mm(
            safe_float(data["davis_current_observation"].get("rain_day_in"))
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
            logging.warning(f"Discarding daily data. Observation time: {observation_time}, Local time: {datetime.datetime.now(tz=local_timezone)}")

        return wr
    
    
    @staticmethod
    def curl_endpoint(endpoint: str, user_did: str, password: str, apiToken: str) -> str:
        response = requests.get(endpoint, {
            "user": user_did,
            "pass": password,
            "apiToken": apiToken
        })
        
        logging.info(f"Requesting {response.url}")
        
        return response.text

    
    @staticmethod
    def get_data(endpoint: str, params: tuple = (), station_id: str = None, data_timezone: tzinfo = timezone.utc, local_timezone: tzinfo = timezone.utc) -> dict:
        assert params[0] is not None
        assert params[1] is not None
        assert params[2] is not None
        
        response = WeatherLinkV1Reader.curl_endpoint(endpoint, params[0], params[2], params[1])#did, password, apiToken are field1, field3, field2
        parsed = WeatherLinkV1Reader.parse(response, data_timezone, local_timezone)
        return parsed
