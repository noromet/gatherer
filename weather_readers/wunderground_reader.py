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

        wr = WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=local_observation_time,
            temperature=live_data["metric"]["temp"],
            wind_speed=live_data["metric"]["windSpeed"],
            max_wind_speed=None,
            wind_direction=live_data["winddir"] if "winddir" in live_data else None,
            rain=live_data["metric"]["precipRate"],
            cumulativeRain=live_data["metric"]["precipTotal"],
            humidity=live_data["humidity"],
            pressure=live_data["metric"]["pressure"],
            flagged=False,
            gathererRunId=None,
            maxTemp=None,
            minTemp=None,
            maxWindGust=None
        )


        if use_daily:
            wr.maxWindGust = last_daily_data["metric"]["windgustHigh"]
            wr.max_wind_speed = last_daily_data["metric"]["windspeedHigh"]
            wr.maxTemp = last_daily_data["metric"]["tempHigh"]
            wr.minTemp = last_daily_data["metric"]["tempLow"]

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
        
        #print full url
        print(f"Requesting {response.url}")
        
        return response.text
    
    @staticmethod
    def get_data(live_endpoint: str, daily_endpoint: str, params: tuple = (), station_id: str = None, data_timezone: tzinfo = timezone.utc, local_timezone: tzinfo = timezone.utc) -> dict:
        assert params[0] is not None #did
        assert params[1] is not None #apiToken
        
        if params[2] not in (None, "NA", "na", ""):
            print("Warning: WundergroundReader does not use password, but it was provided.")
            print("\t It is, however, expected to be required in the future.")

        live_response = WundergroundReader.curl_endpoint(live_endpoint, params[0], params[1])

        # with open(f"./debug/{station_id}_live.json", "w") as f:
        #     f.write(live_response)

        daily_response = WundergroundReader.curl_endpoint(daily_endpoint, params[0], params[1])

        # with open(f"./debug/{station_id}_daily.json", "w") as f:
        #     f.write(daily_response)

        parsed = WundergroundReader.parse(live_response, daily_response, data_timezone=data_timezone, local_timezone=local_timezone)
        return parsed