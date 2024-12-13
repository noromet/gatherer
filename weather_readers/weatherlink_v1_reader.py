from schema import WeatherRecord
from .utils import is_date_too_old, UnitConverter
import json
import requests
import datetime
import logging

# https://api.weather.com/v2/pws/observations/current?stationId=ISOTOYAM2&apiKey=317bd2820daf46edbbd2820daf26ede4&format=json&units=s&numericPrecision=decimal

def safe_float(value):
    return float(value) if value is not None else None

class WeatherLinkV1Reader:
    @staticmethod
    def parse(str_data: str, station_id: str = None) -> WeatherRecord:
        try:
            data = json.loads(str_data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        observation_time = datetime.datetime.strptime(data["observation_time_rfc822"], "%a, %d %b %Y %H:%M:%S %z")
        
        if is_date_too_old(observation_time):
            raise ValueError(f"Record timestamp is too old to be stored as current. Observation time: {observation_time}, local time: {datetime.datetime.now()}")
        
        temperature = data.get("temp_c", UnitConverter.fahrenheit_to_celsius(safe_float(data["davis_current_observation"].get("temp_in_f"))))
        
        wr = WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=observation_time,
            temperature=temperature,
            wind_speed=UnitConverter.mph_to_kph(safe_float(data.get("wind_mph"))),
            wind_direction=data.get("wind_degrees"),
            max_wind_speed=None,
            rain=UnitConverter.inches_to_mm(safe_float(data["davis_current_observation"].get("rain_rate_in_per_hr"))),
            cumulativeRain=None,
            humidity=data.get("relative_humidity"),
            pressure=data.get("pressure_mb"), #mb = hpa
            flagged=False,
            gathererRunId=None,
            maxTemp=None,
            minTemp=None,
            maxWindGust=None
        )

        obstime_local_tz = observation_time.astimezone(datetime.datetime.now().astimezone().tzinfo)

        if not (observation_time.hour == 0 and observation_time.minute < 15) \
            and obstime_local_tz.date() == datetime.datetime.now().date():
            wr.max_wind_speed = UnitConverter.mph_to_kph(safe_float(data["davis_current_observation"].get("wind_day_high_mph")))
            wr.maxWindGust = UnitConverter.mph_to_kph(safe_float(data["davis_current_observation"].get("wind_ten_min_gust_mph")))

            max_float_temp = safe_float(data["davis_current_observation"].get("temp_day_high_f"))
            wr.maxTemp = UnitConverter.fahrenheit_to_celsius(max_float_temp)

            min_float_temp = safe_float(data["davis_current_observation"].get("temp_day_low_f"))
            wr.minTemp = UnitConverter.fahrenheit_to_celsius(min_float_temp)

            wr.cumulativeRain = UnitConverter.inches_to_mm(safe_float(data["davis_current_observation"].get("rain_day_in")))
        else:
            logging.warning(f"[{station_id}]: Discarding daily data. Observation time: {observation_time}, Local time: {obstime_local_tz}")

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
    def get_data(endpoint: str, params: tuple = (), station_id: str = None) -> dict:
        assert params[0] is not None
        assert params[1] is not None
        assert params[2] is not None
        
        response = WeatherLinkV1Reader.curl_endpoint(endpoint, params[0], params[2], params[1])#did, password, apiToken are field1, field3, field2
        parsed = WeatherLinkV1Reader.parse(response, station_id)
        return parsed