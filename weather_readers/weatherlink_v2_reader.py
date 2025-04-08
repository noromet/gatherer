# https://api.weatherlink.com/v2/current/{station-id}?api-key={YOUR API KEY}

from schema import WeatherRecord, WeatherStation
from .weather_reader import WeatherReader
import json
import requests
import datetime
import logging
from datetime import timezone

from .utils import UnitConverter

class WeatherlinkV2Reader(WeatherReader):
    def __init__(self, live_endpoint: str, daily_endpoint: str):
        self.live_endpoint=live_endpoint
        self.daily_endpoint=daily_endpoint
        self.required_fields = ["field1", "field2", "field3"]

    def handle_current_data(self, current: list) -> dict:
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
            "wind_gust": {
                "wind_speed_hi_last_10_min": [],
                "wind_gust": []
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

        timestamp = self.max_or_none(live_response_keys["timestamp"]["ts"])

        temperature = self.coalesce([self.coalesce(live_response_keys["temperature"]["temp"]), self.coalesce(live_response_keys["temperature"]["temp_out"])])
        wind_speed = self.coalesce([self.coalesce(live_response_keys["wind_speed"]["wind_speed"]), self.max_or_none(live_response_keys["wind_speed"]["wind_speed_last"])])
        wind_direction = self.coalesce([self.coalesce(live_response_keys["wind_direction"]["wind_dir"]), self.max_or_none(live_response_keys["wind_direction"]["wind_dir_last"])])
        wind_gust = self.coalesce([self.max_or_none(live_response_keys["wind_gust"]["wind_speed_hi_last_10_min"]), self.coalesce(live_response_keys["wind_gust"]["wind_gust"])])
        rain = self.coalesce([self.coalesce(live_response_keys["rain"]["rain_rate_mm"]), self.coalesce(live_response_keys["rain"]["rain_rate_last_mm"])])
        cumulative_rain = self.coalesce([self.max_or_none(live_response_keys["cumulative_rain"]["rain_day_mm"]), self.max_or_none(live_response_keys["cumulative_rain"]["rainfall_daily_mm"])])
        humidity = self.coalesce([self.coalesce(live_response_keys["humidity"]["hum"]), self.coalesce(live_response_keys["humidity"]["hum_out"])])
        pressure = self.coalesce([self.coalesce(live_response_keys["pressure"]["bar"]), self.coalesce(live_response_keys["pressure"]["bar_sea_level"])])  

        return timestamp, temperature, wind_speed, wind_direction, rain, cumulative_rain, humidity, pressure, wind_gust

    def handle_historic_data(self, historic: list) -> dict:
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
            }
        }

        for sensor in historic:
            for data_point in sensor.get("data", []):
                for keyset in historical_response_keys.values():
                    for key in keyset:
                        if key in data_point:
                            keyset[key].append(data_point[key])

        max_wind_speed = self.max_or_none(historical_response_keys["max_wind_speed"]["wind_speed_hi"])
        cumulative_rain = self.max_or_none(historical_response_keys["cumulative_rain"]["rainfall_mm"])
        max_temp = self.max_or_none(historical_response_keys["max_temp"]["temp_hi"])
        min_temp = self.min_or_none(historical_response_keys["min_temp"]["temp_lo"])

        return max_wind_speed, cumulative_rain, max_temp, min_temp

    def parse(self, station: WeatherStation, data: dict) -> WeatherRecord:
        current_data = {}
        daily_data = {}

        try:
            current_data = json.loads(data["live"])
            current_data = current_data.get("sensors", None)

            daily_data = None
            if data["live"]:
                daily_data = json.loads(data["live"])
                daily_data = daily_data.get("sensors", None)

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        timestamp, temperature, wind_speed, wind_direction, rain, cumulative_rain, humidity, pressure, wind_gust = self.handle_current_data(current_data)
        
        if daily_data is not None:
            max_wind_speed, cumulative_rain_historic, max_temp, min_temp = self.handle_historic_data(daily_data)
        else:
            max_wind_speed, cumulative_rain_historic, max_temp, min_temp = None, None, None, None

        final_cumulative_rain = self.coalesce([cumulative_rain, cumulative_rain_historic])

        observation_time = datetime.datetime.fromtimestamp(timestamp, tz=station.data_timezone)
        observation_time_utc = observation_time.astimezone(timezone.utc)
        self.assert_date_age(observation_time_utc)
        local_observation_time = observation_time.astimezone(station.local_timezone)

        wr = WeatherRecord(
            id=None,
            station_id=None,
            source_timestamp=local_observation_time,
            temperature=UnitConverter.fahrenheit_to_celsius(temperature),
            wind_speed=UnitConverter.mph_to_kph(wind_speed),
            max_wind_speed=UnitConverter.mph_to_kph(max_wind_speed),
            wind_direction=wind_direction,
            rain=rain,
            humidity=humidity,
            pressure=UnitConverter.psi_to_hpa(pressure),
            flagged=False,
            gatherer_thread_id=None,
            cumulative_rain=final_cumulative_rain,
            max_temperature=UnitConverter.fahrenheit_to_celsius(max_temp),
            min_temperature=UnitConverter.fahrenheit_to_celsius(min_temp),
            wind_gust=UnitConverter.mph_to_kph(wind_gust),
            max_wind_gust=None
        )

        return wr

    
    def fetch_data(self, station: WeatherStation) -> dict:
        station_id, api_key, api_secret = station.field1, station.field2, station.field3

        live_url = self.live_endpoint.format(mode="current", station_id=station_id)

        params = {
            "api-key": api_key,
            "t": int(datetime.datetime.now().timestamp())
        }
        headers = {'X-Api-Secret': api_secret}
        logging.info(f"Requesting {live_url}")
        live_response = requests.get(live_url, params=params, headers=headers)

        if live_response.status_code != 200:
            logging.error(f"Request failed with status code {live_response.status_code}. Check station connection parameters.")
            return None
        
        daily_url = self.daily_endpoint.format(mode="historic", station_id=station_id)

        _15_minutes_ago = datetime.datetime.now() - datetime.timedelta(minutes=15)
        params = {
            "api-key": api_key,
            "t": int(datetime.datetime.now().timestamp()),
            "start-timestamp": int(_15_minutes_ago.timestamp()),
            "end-timestamp": int(datetime.datetime.now().replace(hour=23, minute=59, second=59, microsecond=0).timestamp())
        }
        headers = {'X-Api-Secret': api_secret}
        logging.info(f"Requesting {daily_url}")
        daily_response = requests.get(daily_url, params=params, headers=headers)

        if daily_response.status_code != 200:
            logging.warning(f"Request failed with status code {daily_response.status_code}. Is the subscription active?")
            daily_response = None

        ret_dict = {"live": live_response.text}

        if daily_response is not None:
            ret_dict["daily"] = daily_response.text

        return ret_dict
    