from schema import WeatherRecord
from .utils import is_date_too_old, safe_float, safe_int
import json
import requests
import datetime

class EcowittReader:
    @staticmethod
    def parse(live_str_data: str, daily_str_data: str, station_id: str = None, timezone: str = "Etc/UTC") -> WeatherRecord:
        try:
            live_data = json.loads(live_str_data)["data"]
            daily_data = json.loads(daily_str_data)["data"]
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        #parse timestamp in seconds
        observation_time = datetime.datetime.fromtimestamp(safe_int(live_data["outdoor"]["temperature"]["time"]))
        
        if is_date_too_old(observation_time):
            raise ValueError(f"[{station_id}]: Record timestamp is too old to be stored as current. Observation time: {observation_time}, local time: {datetime.datetime.now()}")

        wr = WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=observation_time,
            temperature=safe_float(live_data["outdoor"]["temperature"]["value"]),
            wind_speed=safe_float(live_data["wind"]["wind_speed"]["value"]),
            wind_direction=safe_float(live_data["wind"]["wind_direction"]["value"]),
            max_wind_speed=None,
            rain=safe_float(live_data["rainfall"]["rain_rate"]["value"]),
            cumulativeRain=safe_float(live_data["rainfall"]["daily"]["value"]),
            humidity=safe_float(live_data["outdoor"]["humidity"]["value"]),
            pressure=safe_float(live_data["pressure"]["relative"]["value"]),
            flagged=False,
            gathererRunId=None,
            minTemp=None,
            maxTemp=None,
            maxWindGust=None
        )


        #dailies
        max_temp = max(
            safe_float(temp) for temp in daily_data["outdoor"]["temperature"]["list"].values()
        )
        min_temp = min(
            safe_float(temp) for temp in daily_data["outdoor"]["temperature"]["list"].values()
        )
        max_wind_speed = max(
            safe_float(speed) for speed in daily_data["wind"]["wind_speed"]["list"].values()
        )
        max_wind_gust = max(
            safe_float(speed) for speed in daily_data["wind"]["wind_gust"]["list"].values()
        )

        wr.maxTemp = max_temp
        wr.minTemp = min_temp
        wr.max_wind_speed = max_wind_speed
        wr.maxWindGust = max_wind_gust

        return wr


    @staticmethod
    def curl_live_endpoint(endpoint: str, mac: str, api_key: str, application_key: str) -> str:
        url = f"{endpoint}?mac={mac}&api_key={api_key}&application_key={application_key}"
        url += "&temp_unitid=1&pressure_unitid=3&wind_speed_unitid=7&rainfall_unitid=12"
    
        response = requests.get(url)
        
        # Print full URL
        print(f"Requesting {response.url}")
        
        return response.text
    
    @staticmethod
    def curl_daily_endpoint(daily_endpoint: str, mac: str, api_key: str, application_key: str) -> str:
        start_date = datetime.datetime.now().strftime("%Y-%m-%d 00:00:00")
        end_date = datetime.datetime.now().strftime("%Y-%m-%d 23:59:59")
        
        url = f"{daily_endpoint}?mac={mac}&api_key={api_key}&application_key={application_key}"
        url += "&temp_unitid=1&pressure_unitid=3&wind_speed_unitid=7&rainfall_unitid=12"
        url += f"&cycle_type=auto&start_date={start_date}&end_date={end_date}"
        url += "&call_back=outdoor.temperature,outdoor.humidity,wind.wind_speed,wind.wind_gust"

        response = requests.get(url)
        
        # Print full URL
        print(f"Requesting {response.url}")
        
        return response.text

    
    @staticmethod
    def get_data(live_endpoint: str, daily_endpoint: str, params: tuple = (), station_id: str = None, timezone: str = "Etc/UTC") -> WeatherRecord:
        assert params[0] is not None, "station_id is null"  # station id
        assert params[1] is not None, "api_key is null"  # api key
        assert params[2] is not None, "application_key is null"  # application key
        
        live_response = EcowittReader.curl_live_endpoint(live_endpoint, params[0], params[1], params[2])
        daily_response = EcowittReader.curl_daily_endpoint(daily_endpoint, params[0], params[1], params[2])
        
        parsed = EcowittReader.parse(live_response, daily_response, station_id, timezone)

        return parsed