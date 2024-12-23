#https://api.weather.com/v2/pws/observations/current?stationId=ISOTODEL6&format=json&units=e&apiKey=a952662893aa49f992662893aad9f98d

from schema import WeatherRecord
from .utils import is_date_too_old
import json
import requests
import datetime
import logging

class WundergroundReader:
    @staticmethod
    def parse(live_data_str: str, daily_data_str: str, station_id: str = None, timezone: str = "Etc/UTC") -> WeatherRecord:
        try:
            live_data = json.loads(live_data_str)["observations"][0]
            last_daily_data = json.loads(daily_data_str)["summaries"][-1]

            assert live_data["stationID"] == last_daily_data["stationID"], "Something broke: live and daily data are not from the same station."
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        observation_time = datetime.datetime.strptime(live_data["obsTimeLocal"], "%Y-%m-%d %H:%M:%S")
        observation_time_utc = datetime.datetime.strptime(live_data["obsTimeUtc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
        
        if is_date_too_old(observation_time_utc):
            raise ValueError("Record timestamp is too old to be stored as current. Observation time: {observation_time}, local time: {datetime.datetime.now()}")

        wr = WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=observation_time,
            temperature=live_data["metric"]["temp"],
            wind_speed=live_data["metric"]["windSpeed"],
            max_wind_speed=None,
            wind_direction=live_data["winddir"] if "winddir" in live_data else None,
            rain=live_data["metric"]["precipRate"],
            cumulativeRain=None,
            humidity=live_data["humidity"],
            pressure=live_data["metric"]["pressure"],
            flagged=False,
            gathererRunId=None,
            maxTemp=None,
            minTemp=None,
            maxWindGust=None
        )

        now_in_utc = datetime.datetime.now(tz=datetime.timezone.utc)
        # records from before 00:15 are still yesterday's, so discard. also, discard those with obsTimeUtc not the same day as today in local time
        if not (observation_time.hour == 0 and observation_time.minute < 15) \
            and observation_time.date() == datetime.datetime.now().date():
            
            wr.maxWindGust = last_daily_data["metric"]["windgustHigh"]
            wr.max_wind_speed = last_daily_data["metric"]["windspeedHigh"]
            wr.maxTemp = last_daily_data["metric"]["tempHigh"]
            wr.minTemp = last_daily_data["metric"]["tempLow"]
            wr.cumulativeRain = last_daily_data["metric"]["precipTotal"]

        else:
            logging.warning(f"[{station_id}]: Discarding daily data. Observation time: {observation_time}, now in UTC: {now_in_utc}")

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
        
        #print full url
        print(f"Requesting {response.url}")
        
        return response.text
    
    @staticmethod
    def get_data(live_endpoint: str, daily_endpoint: str, params: tuple = (), station_id: str = None, timezone: str = "Etc/UTC") -> dict:
        assert params[0] is not None #did
        assert params[1] is not None #apiToken
        
        if params[2] not in (None, "NA", "na", ""):
            print("Warning: WundergroundReader does not use password, but it was provided.")
            print("\t It is, however, expected to be required in the future.")

        live_response = WundergroundReader.curl_endpoint(live_endpoint, params[0], params[1])
        daily_response = WundergroundReader.curl_endpoint(daily_endpoint, params[0], params[1])

        parsed = WundergroundReader.parse(live_response, daily_response, station_id=station_id, timezone=timezone)
        return parsed