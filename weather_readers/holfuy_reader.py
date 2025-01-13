from schema import WeatherRecord
from .utils import UnitConverter
from .common import assert_date_age
import json
import requests
import datetime
import logging
from datetime import tzinfo, timezone

class HolfuyReader:
    @staticmethod
    def parse(str_data: str, station_id: str = None, timezone: tzinfo = timezone.utc) -> WeatherRecord:
        try:
            data = json.loads(str_data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        observation_time = datetime.datetime.strptime(data["dateTime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone)
        observation_time_utc = observation_time.astimezone(datetime.timezone.utc)
        
        assert_date_age(observation_time_utc)

        current_date = datetime.datetime.now(timezone).date()
        observation_date = observation_time.date()

        if observation_time.time() >= datetime.time(0, 0) and observation_time.time() <= datetime.time(0, 15) and observation_date == current_date:
            use_daily = False
        else:
            use_daily = True

        wr = WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=observation_time,
            temperature=data["temperature"],  # Already in Celsius
            wind_speed=UnitConverter.mph_to_kph(data["wind"]["speed"]),
            wind_direction=data["wind"]["direction"],
            max_wind_speed=None,
            rain=data["rain"],  # Assuming rain is in mm
            cumulativeRain=None,  # Assuming rain is in mm
            humidity=data["humidity"],
            pressure=data["pressure"],  # Assuming pressure is in hPa
            flagged=False,
            gathererRunId=None,
            minTemp=None,
            maxTemp=None,
            maxWindGust=None
        )

        if use_daily:
            wr.maxWindGust = UnitConverter.mph_to_kph(data["wind"]["gust"])
            wr.minTemp = data["daily"]["min_temp"]
            wr.maxTemp = data["daily"]["max_temp"]
            wr.cumulativeRain = round(data["daily"]["sum_rain"], 2)

        return wr
    
    @staticmethod
    def curl_endpoint(endpoint: str, station_id: str, password: str) -> str:
        endpoint = f"{endpoint}?s={station_id}&pw={password}&m=JSON&tu=C&su=m/s&daily=True"

        response = requests.get(endpoint)
        
        # Print full URL
        print(f"Requesting {response.url}")
        
        return response.text

    
    @staticmethod
    def get_data(endpoint: str, params: tuple = (), station_id: str = None, timezone: tzinfo = timezone.utc) -> WeatherRecord:
        assert params[0] is not None, "station_id is null"  # station id
        assert params[2] is not None, "password is null"  # password
        
        if params[1] not in (None, "NA", "na", ""):
            print("Warning: HolfuyReader does not use api key, but it was provided.")
            logging.warning(f"{[station_id]} Warning: HolfuyReader does not use api key, but it was provided.")

        response = HolfuyReader.curl_endpoint(endpoint, params[0], params[2])
        parsed = HolfuyReader.parse(response, station_id, timezone)
        return parsed