from schema import WeatherRecord
from .utils import safe_float, safe_int
from .common import assert_date_age
import json
import requests
import datetime
from datetime import tzinfo, timezone
import logging

class EcowittReader:
    @staticmethod
    def parse(live_str_data: str, daily_str_data: str, data_timezone: tzinfo = timezone.utc, local_timezone: tzinfo = timezone.utc) -> WeatherRecord:
        try:
            live_data = json.loads(live_str_data)["data"]
            daily_data = json.loads(daily_str_data)["data"]
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        #parse timestamp in seconds
        observation_time = datetime.datetime.fromtimestamp(safe_int(live_data["outdoor"]["temperature"]["time"])).replace(tzinfo=data_timezone)
        observation_time_utc = observation_time.astimezone(timezone.utc)
        assert_date_age(observation_time_utc)

        local_observation_time = observation_time.astimezone(local_timezone)
        current_date = datetime.datetime.now(tz=data_timezone).date()
        observation_date = observation_time.date()
        if observation_time.time() >= datetime.time(0, 0) and observation_time.time() <= datetime.time(0, 15) and observation_date == current_date:
            use_daily = False
        else:
            use_daily = True
        
        outdoor = live_data.get("outdoor", {})
        wind = live_data.get("wind", {})
        rainfall = live_data.get("rainfall", {})

        temperature = outdoor.get("temperature", {}).get("value")
        wind_speed = wind.get("wind_speed", {}).get("value")
        wind_direction = wind.get("wind_direction", {}).get("value")
        rain = rainfall.get("rain_rate", {}).get("value")
        cumulative_rain = rainfall.get("daily", {}).get("value")
        humidity = outdoor.get("humidity", {}).get("value")
        pressure = live_data.get("pressure", {}).get("relative", {}).get("value")
        maxWindGust = wind.get("wind_gust", {}).get("value")

        wr = WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=local_observation_time,
            temperature=safe_float(temperature),
            wind_speed=safe_float(wind_speed),
            wind_direction=safe_float(wind_direction),
            max_wind_speed=None,
            rain=safe_float(rain),
            cumulativeRain=safe_float(cumulative_rain),
            humidity=safe_float(humidity),
            pressure=safe_float(pressure),
            flagged=False,
            gathererRunId=None,
            minTemp=None,
            maxTemp=None,
            maxWindGust=safe_float(maxWindGust),
            maxMaxWindGust=None
        )

        if use_daily:
            max_temp = max(
                safe_float(temp) for temp in daily_data["outdoor"]["temperature"]["list"].values()
            )
            min_temp = min(
                safe_float(temp) for temp in daily_data["outdoor"]["temperature"]["list"].values()
            )
            max_wind_speed = max(
                safe_float(speed) for speed in daily_data["wind"]["wind_speed"]["list"].values()
            )
            max_max_wind_gust = max(
                safe_float(gust) for gust in daily_data["wind"]["wind_gust"]["list"].values()
            )

            wr.maxTemp = max_temp
            wr.minTemp = min_temp
            wr.max_wind_speed = max_wind_speed
            wr.maxMaxWindGust = max_max_wind_gust

        return wr


    @staticmethod
    def curl_live_endpoint(endpoint: str, mac: str, api_key: str, application_key: str) -> str:
        url = f"{endpoint}?mac={mac}&api_key={api_key}&application_key={application_key}"
        url += "&temp_unitid=1&pressure_unitid=3&wind_speed_unitid=7&rainfall_unitid=12"
    
        response = requests.get(url)
        
        logging.info(f"Requesting {response.url}")
        
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
        
        logging.info(f"Requesting {response.url}")
        
        return response.text

    
    @staticmethod
    def get_data(live_endpoint: str, daily_endpoint: str, params: tuple = (), station_id: str = None, data_timezone: tzinfo = timezone.utc, local_timezone: tzinfo = timezone.utc) -> WeatherRecord:
        assert params[0] is not None, "station_id is null"  # station id
        assert params[1] is not None, "api_key is null"  # api key
        assert params[2] is not None, "application_key is null"  # application key
        
        live_response = EcowittReader.curl_live_endpoint(live_endpoint, params[0], params[1], params[2])
        daily_response = EcowittReader.curl_daily_endpoint(daily_endpoint, params[0], params[1], params[2])
        
        parsed = EcowittReader.parse(live_response, daily_response, data_timezone, local_timezone)

        return parsed