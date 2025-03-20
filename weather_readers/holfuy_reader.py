from schema import WeatherRecord
from .common import assert_date_age
import json
import requests
import datetime
import logging
from datetime import tzinfo, timezone

class HolfuyReader:
    @staticmethod
    def parse(live_data: str, historic_data: str, data_timezone: tzinfo = timezone.utc, local_timezone: tzinfo = timezone.utc) -> WeatherRecord:
        try:
            live_data = json.loads(live_data)
            historic_data = json.loads(historic_data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        observation_time = datetime.datetime.strptime(live_data["dateTime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=data_timezone)
        observation_time_utc = observation_time.astimezone(timezone.utc)
        assert_date_age(observation_time_utc)
        local_observation_time = observation_time.astimezone(local_timezone)

        current_date = datetime.datetime.now(tz=data_timezone).date()
        observation_date = observation_time.date()

        if observation_time.time() >= datetime.time(0, 0) and observation_time.time() <= datetime.time(0, 15) and observation_date == current_date:
            use_daily = False
        else:
            use_daily = True

        wr = WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=local_observation_time,
            temperature=live_data["temperature"],  # Already in Celsius
            wind_speed=live_data["wind"]["speed"],
            wind_direction=live_data["wind"]["direction"],
            max_wind_speed=None,
            rain=live_data["rain"],  # Assuming rain is in mm
            cumulativeRain=None,  # Assuming rain is in mm
            humidity=live_data["humidity"],
            pressure=live_data["pressure"],  # Assuming pressure is in hPa
            flagged=False,
            gathererRunId=None,
            minTemp=None,
            maxTemp=None,
            maxWindGust=None,
            maxMaxWindGust=None,
        )

        if use_daily:
            wr.maxWindGust = historic_data["measurements"][0]["wind"]["gust"]
            wr.minTemp = live_data["daily"]["min_temp"]
            wr.maxTemp = live_data["daily"]["max_temp"]
            wr.cumulativeRain = round(live_data["daily"]["sum_rain"], 2)
            wr.maxMaxWindGust = historic_data["measurements"][0]["wind"]["gust"]
        else:
            logging.info(f"Discarding daily data. Observation time: {observation_time}, Local time: {datetime.datetime.now(tz=local_timezone)}")
            wr.minTemp = None
            wr.maxTemp = None
            wr.cumulativeRain = None
            wr.maxWindGust = None
            wr.maxMaxWindGust = None

        return wr
    
    @staticmethod
    def curl_live_endpoint(endpoint: str, station_id: str, password: str) -> str:
        endpoint = f"{endpoint}?s={station_id}&pw={password}&m=JSON&tu=C&su=km/h&daily=True"

        response = requests.get(endpoint)
        
        logging.info(f"Requesting {response.url}")
        
        return response.text
    
    @staticmethod
    def curl_historic_endpoint(endpoint: str, station_id: str, password: str) -> str:
        endpoint = f"{endpoint}?s={station_id}&pw={password}&m=JSON&tu=C&su=km/h&type=2&mback=60"

        response = requests.get(endpoint)
    
        logging.info(f"Requesting {response.url}")
        
        return response.text

    
    @staticmethod
    def get_data(live_endpoint: str, historic_endpoint: str, params: tuple = (), station_id: str = None, data_timezone: tzinfo = timezone.utc, local_timezone: tzinfo = timezone.utc) -> WeatherRecord:
        assert params[0] is not None, "station_id is null"  # station id
        assert params[2] is not None, "password is null"  # password
        
        if params[1] not in (None, "NA", "na", ""):
            logging.warning(f"{[station_id]} Warning: HolfuyReader does not use api key, but it was provided.")

        live_response = HolfuyReader.curl_live_endpoint(live_endpoint, params[0], params[2])

        historical_response = HolfuyReader.curl_historic_endpoint(historic_endpoint, params[0], params[2])

        parsed = HolfuyReader.parse(live_data=live_response, historic_data=historical_response, data_timezone=data_timezone, local_timezone=local_timezone)
        return parsed