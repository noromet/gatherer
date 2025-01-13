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
    def parse(str_data: str, station_id: str = None, timezone: tzinfo = timezone.utc) -> WeatherRecord:
        try:
            data = json.loads(str_data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        observation_time = datetime.datetime.strptime(data["observation_time_rfc822"], "%a, %d %b %Y %H:%M:%S %z").replace(tzinfo=timezone)
        observation_time_utc = observation_time.astimezone(datetime.timezone.utc)
        
        assert_date_age(observation_time_utc)
    
        current_date = datetime.datetime.now(timezone).date()
        observation_date = observation_time.date()

        if observation_time.time() >= datetime.time(0, 0) and observation_time.time() <= datetime.time(0, 15) and observation_date == current_date:
            use_daily = False
        else:
            use_daily = True
        
        temperature = data.get("temp_c", None)
        
        wr = WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=observation_time,
            temperature=safe_float(temperature),
            wind_speed=UnitConverter.mph_to_kph(safe_float(data.get("wind_mph"))),
            wind_direction=safe_float(data.get("wind_degrees")),
            max_wind_speed=None,
            rain=UnitConverter.inches_to_mm(safe_float(data["davis_current_observation"].get("rain_rate_in_per_hr"))),
            cumulativeRain=None,
            humidity=safe_float(data.get("relative_humidity")),
            pressure=safe_float(data.get("pressure_mb")), #mb = hpa
            flagged=False,
            gathererRunId=None,
            maxTemp=None,
            minTemp=None,
            maxWindGust=None
        )

        if use_daily:
            wr.max_wind_speed = UnitConverter.mph_to_kph(
                safe_float(data["davis_current_observation"].get("wind_day_high_mph"))
            )
            wr.maxWindGust = UnitConverter.mph_to_kph(
                safe_float(data["davis_current_observation"].get("wind_ten_min_gust_mph"))
            )

            wr.maxTemp = UnitConverter.fahrenheit_to_celsius(
                safe_float(data["davis_current_observation"].get("temp_day_high_f"))
            )

            wr.minTemp = UnitConverter.fahrenheit_to_celsius(
                safe_float(data["davis_current_observation"].get("temp_day_low_f"))
            )

            wr.cumulativeRain = UnitConverter.inches_to_mm(
                safe_float(data["davis_current_observation"].get("rain_day_in"))
            )
        else:
            logging.warning(f"[{station_id}]: Discarding daily data. Observation time: {observation_time}, Local time: {datetime.datetime.now(timezone)}")

        return wr
    
    
    @staticmethod
    def curl_endpoint(endpoint: str, user_did: str, password: str, apiToken: str) -> str:
        response = requests.get(endpoint, {
            "user": user_did,
            "pass": password,
            "apiToken": apiToken
        })
        
        #print full url
        print(f"Requesting {response.url}")
        
        return response.text

    
    @staticmethod
    def get_data(endpoint: str, params: tuple = (), station_id: str = None, timezone: tzinfo = timezone.utc) -> dict:
        assert params[0] is not None
        assert params[1] is not None
        assert params[2] is not None
        
        response = WeatherLinkV1Reader.curl_endpoint(endpoint, params[0], params[2], params[1])#did, password, apiToken are field1, field3, field2
        parsed = WeatherLinkV1Reader.parse(response, station_id, timezone)
        return parsed
