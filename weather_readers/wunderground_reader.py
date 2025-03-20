#https://api.weather.com/v2/pws/observations/current?stationId=ISOTODEL6&format=json&units=e&apiKey=a952662893aa49f992662893aad9f98d

from schema import WeatherRecord
from .common import assert_date_age
import json
import requests
import datetime
import logging
from datetime import tzinfo, timezone

class WundergroundReader:
    @staticmethod
    def parse(live_data_str: str, daily_data_str: str, data_timezone: tzinfo = timezone.utc, local_timezone: tzinfo = timezone.utc) -> WeatherRecord:
        try:
            live_data = json.loads(live_data_str)["observations"][0]
            last_daily_data = json.loads(daily_data_str)["summaries"][-1]

            assert live_data["stationID"] == last_daily_data["stationID"], "Something broke: live and daily data are not from the same station."
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        # datetime management
        observation_time = datetime.datetime.strptime(live_data["obsTimeLocal"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=data_timezone)
        observation_time_utc = datetime.datetime.strptime(live_data["obsTimeUtc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        assert_date_age(observation_time_utc)
        
        local_observation_time = observation_time.astimezone(local_timezone)
        current_date = datetime.datetime.now(tz=data_timezone).date()
        observation_date = observation_time.date()
        if observation_time.time() >= datetime.time(0, 0) and observation_time.time() <= datetime.time(0, 15) and observation_date == current_date:
            use_daily = False
        else:
            use_daily = True
        ##

        live_metric_data = live_data.get("metric") #esquizo
        if live_metric_data is None:
            raise ValueError("No metric data found in live data.")
        
        wr = WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=local_observation_time,
            temperature=live_metric_data.get("temp", None),
            wind_speed=live_metric_data.get("windSpeed", None),
            max_wind_speed=None,
            wind_direction=live_data.get("winddir", None),
            rain=live_metric_data.get("precipRate", None),
            cumulativeRain=live_metric_data.get("precipTotal", None),
            humidity=live_data.get("humidity", None),
            pressure=live_metric_data.get("pressure", None),
            flagged=False,
            gathererRunId=None,
            maxTemp=None,
            minTemp=None,
            maxWindGust=live_metric_data.get("windGust", None),
            maxMaxWindGust=None
        )


        daily_metric_data = last_daily_data.get("metric")
        if use_daily:
            # wr.maxWindGust = daily_metric_data.get("windgustHigh", None)
            wr.max_wind_speed = daily_metric_data.get("windspeedHigh", None)
            wr.maxTemp = daily_metric_data.get("tempHigh", None)
            wr.minTemp = daily_metric_data.get("tempLow", None)
            wr.maxMaxWindGust = daily_metric_data.get("windgustHigh", None)

            print

        else:
            logging.warning(f"Discarding daily data. Observation time: {observation_time}, Local time: {datetime.datetime.now(tz=local_timezone)}")

        return wr
    
    @staticmethod
    def curl_endpoint(endpoint: str, did: str, token: str) -> str:
        response = requests.get(endpoint, {
            "stationId": did,
            "apiKey": token,
            "format": "json",
            "units": "m",
            "numericPrecision": "decimal"
        })
        
        logging.info(f"Requesting {response.url}")
        
        return response.text
    
    @staticmethod
    def get_data(live_endpoint: str, daily_endpoint: str, params: tuple = (), station_id: str = None, data_timezone: tzinfo = timezone.utc, local_timezone: tzinfo = timezone.utc) -> dict:
        assert params[0] is not None #did
        assert params[1] is not None #apiToken
        
        if params[2] not in (None, "NA", "na", ""):
            logging.warning("Warning: WundergroundReader does not use password, but it was provided. It is, however, expected to be required in the future.")

        live_response = WundergroundReader.curl_endpoint(live_endpoint, params[0], params[1])

        daily_response = WundergroundReader.curl_endpoint(daily_endpoint, params[0], params[1])

        parsed = WundergroundReader.parse(live_response, daily_response, data_timezone=data_timezone, local_timezone=local_timezone)
        return parsed