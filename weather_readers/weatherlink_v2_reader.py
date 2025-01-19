# https://api.weatherlink.com/v2/current/{station-id}?api-key={YOUR API KEY}

from schema import WeatherRecord
from .utils import UnitConverter, coalesce, max_or_none, min_or_none
from .common import assert_date_age
import json
import requests
import datetime
import logging
from datetime import tzinfo, timezone

def handle_current_data(current: list) -> dict:
    live_response_keys = {
        "timestamp": {
            "ts": []   
        },
        "temperature": {
            "temp": [],
            "temp_out": []
        },
        "wind_speed": {
            "wind_speed": [],
            "wind_speed_last": []
        },
        "wind_direction": {
            "wind_dir": [],
            "wind_dir_last": []
        },
        "rain": {
            "rain_rate_mm": [],
            "rain_rate_last_mm": [],
        },
        "cumulative_rain": {
            "rain_day_mm": [],
            "rainfall_daily_mm": []
        },
        "humidity": {
            "hum": [],
            "hum_out": []
        },
        "pressure": {
            "bar": [],
            "bar_sea_level": [],
        }
    }

    for sensor in current:
        for data_point in sensor.get("data", []):
            for keyset in live_response_keys.values():
                for key in keyset:
                    if key in data_point:
                        keyset[key].append(data_point[key])

    timestamp = max_or_none(live_response_keys["timestamp"]["ts"])

    temperature = coalesce([coalesce(live_response_keys["temperature"]["temp"]), coalesce(live_response_keys["temperature"]["temp_out"])])
    wind_speed = coalesce([coalesce(live_response_keys["wind_speed"]["wind_speed"]), max_or_none(live_response_keys["wind_speed"]["wind_speed_last"])])
    wind_direction = coalesce([coalesce(live_response_keys["wind_direction"]["wind_dir"]), max_or_none(live_response_keys["wind_direction"]["wind_dir_last"])])
    rain = coalesce([coalesce(live_response_keys["rain"]["rain_rate_mm"]), coalesce(live_response_keys["rain"]["rain_rate_last_mm"])])
    cumulative_rain = coalesce([max_or_none(live_response_keys["cumulative_rain"]["rain_day_mm"]), max_or_none(live_response_keys["cumulative_rain"]["rainfall_daily_mm"])])
    humidity = coalesce([coalesce(live_response_keys["humidity"]["hum"]), coalesce(live_response_keys["humidity"]["hum_out"])])
    pressure = coalesce([coalesce(live_response_keys["pressure"]["bar"]), coalesce(live_response_keys["pressure"]["bar_sea_level"])])
    
    return timestamp, temperature, wind_speed, wind_direction, rain, cumulative_rain, humidity, pressure

def handle_historic_data(historic: list) -> dict:
    historical_response_keys = {
        "max_wind_speed": {
            "wind_speed_hi": [],
        },
        "cumulative_rain": {
            "rainfall_mm": [],
        },
        "max_temp": {
            "temp_hi": []
        },
        "min_temp": {
            "temp_lo": []
        },
        "max_wind_gust": {
            "wind_speed_hi": []
        }
    }

    for sensor in historic:
        for data_point in sensor.get("data", []):
            for keyset in historical_response_keys.values():
                for key in keyset:
                    if key in data_point:
                        keyset[key].append(data_point[key])

    max_wind_speed = max_or_none(historical_response_keys["max_wind_speed"]["wind_speed_hi"])
    cumulative_rain = max_or_none(historical_response_keys["cumulative_rain"]["rainfall_mm"])
    max_temp = max_or_none(historical_response_keys["max_temp"]["temp_hi"])
    min_temp = min_or_none(historical_response_keys["min_temp"]["temp_lo"])
    max_wind_gust = max_or_none(historical_response_keys["max_wind_gust"]["wind_speed_hi"])

    return max_wind_speed, cumulative_rain, max_temp, min_temp, max_wind_gust

class WeatherlinkV2Reader:
    @staticmethod
    def parse(current_str_data: str, historic_str_data: str, data_timezone: tzinfo = timezone.utc, local_timezone: tzinfo = timezone.utc) -> WeatherRecord:
        try:
            current_data = json.loads(current_str_data)
            current_data = current_data.get("sensors", None)

            historic_data = None
            if historic_str_data is not None:
                historic_data = json.loads(historic_str_data)
                historic_data = historic_data.get("sensors", None)

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        timestamp, temperature, wind_speed, wind_direction, rain, cumulative_rain, humidity, pressure = handle_current_data(current_data)
        
        if historic_data is not None:
            max_wind_speed, cumulative_rain_historic, max_temp, min_temp, max_wind_gust = handle_historic_data(historic_data)
        else:
            max_wind_speed, cumulative_rain_historic, max_temp, min_temp, max_wind_gust = None, None, None, None, None

        final_cumulative_rain = coalesce([cumulative_rain, cumulative_rain_historic])

        observation_time = datetime.datetime.fromtimestamp(timestamp, tz=data_timezone)
        observation_time_utc = observation_time.astimezone(timezone.utc)
        assert_date_age(observation_time_utc)
        local_observation_time = observation_time.astimezone(local_timezone)

        wr = WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=local_observation_time,
            temperature=UnitConverter.fahrenheit_to_celsius(temperature),
            wind_speed=UnitConverter.mph_to_kph(wind_speed),
            wind_direction=wind_direction,
            max_wind_speed=UnitConverter.mph_to_kph(max_wind_speed),
            rain=rain,
            cumulativeRain=final_cumulative_rain,
            humidity=humidity,
            pressure=UnitConverter.psi_to_hpa(pressure),
            flagged=False,
            gathererRunId=None,
            maxTemp=UnitConverter.fahrenheit_to_celsius(max_temp),
            minTemp=UnitConverter.fahrenheit_to_celsius(min_temp),
            maxWindGust=max_wind_gust
        )

        return wr

    @staticmethod
    def curl_current_endpoint(endpoint: str, station_id: str, api_key: str, api_secret: str) -> str:
        endpoint = endpoint.format(mode="current", station_id=station_id)

        params = {
            "api-key": api_key,
            "t": int(datetime.datetime.now().timestamp())
        }
        headers = {
            'X-Api-Secret': api_secret
        }
        response = requests.get(endpoint, params=params, headers=headers)
        
        #print full url
        print(f"Requesting {response.url}")

        if response.status_code != 200:
            logging.error(f"Request failed with status code {response.status_code}. Check station connection parameters.")
            return None
        
        # with open(f"./debug/{station_id}_current.json", "w") as f:
        #     f.write(response.text)

        return response.text

    @staticmethod
    def curl_historic_endpoint(endpoint: str, station_id: str, api_key: str, api_secret: str) -> str:
        endpoint = endpoint.format(mode="historic", station_id=station_id)

        #start timestamp is today 5 minutes ago, end timestamp is today at 23:59:59
        _15_minutes_ago = datetime.datetime.now() - datetime.timedelta(minutes=15)

        params = {
            "api-key": api_key,
            "t": int(datetime.datetime.now().timestamp()),
            "start-timestamp": int(_15_minutes_ago.timestamp()),
            "end-timestamp": int(datetime.datetime.now().replace(hour=23, minute=59, second=59, microsecond=0).timestamp())
        }
        headers = {
            'X-Api-Secret': api_secret
        }
        response = requests.get(endpoint, params=params, headers=headers)
        
        #print full url
        print(f"Requesting {response.url}")

        if response.status_code != 200:
            logging.warning(f"Request failed with status code {response.status_code}. Is the subscription active?")
            return None

        # with open(f"./debug/{station_id}_historic.json", "w") as f:
        #     f.write(response.text)

        return response.text
    
    @staticmethod
    def get_data(endpoint: str, params: tuple = (), station_id: str = None, data_timezone: tzinfo = timezone.utc, local_timezone: tzinfo = timezone.utc) -> dict:
        assert params[0] is not None, "station id is null"
        assert params[1] is not None, "api key is null"
        assert params[2] is not None, "api secret is null"
        
        current_response = WeatherlinkV2Reader.curl_current_endpoint(endpoint, params[0], params[1], params[2])
        historic_response = WeatherlinkV2Reader.curl_historic_endpoint(endpoint, params[0], params[1], params[2])

        if current_response is None:
            return None

        parsed = WeatherlinkV2Reader.parse(current_response, historic_response, data_timezone=data_timezone, local_timezone=local_timezone)

        return parsed
