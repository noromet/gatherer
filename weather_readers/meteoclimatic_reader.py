from schema import WeatherRecord, WeatherStation
import requests
import logging
import json
from datetime import timezone
from .weather_reader import WeatherReader


class MeteoclimaticReader(WeatherReader):
    def __init__(self):
        self.required_fields = ["field1", "field2"]

    def parse(self, station: WeatherStation, data: dict) -> WeatherRecord:
        live_data = data["live"]

        if not live_data:
            raise ValueError("No data received from the station.")

        data = {}
        for line in live_data.strip().split("*"):
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, value = line.split("=")
            key = key.strip()
            value = value.strip()

            if key in CODE_TO_NAME.keys() and key in WHITELIST:
                data[CODE_TO_NAME[key]] = value

        data["record_timestamp"] = self.smart_parse_datetime(
            data["record_timestamp"], timezone=station.data_timezone
        )
        if data["record_timestamp"] is None:
            raise ValueError("Cannot accept a reading without a timestamp.")

        observation_time_utc = data["record_timestamp"].astimezone(timezone.utc)
        self.assert_date_age(observation_time_utc)

        local_observation_time = observation_time_utc.astimezone(station.local_timezone)

        try:
            wind_direction = self.smart_azimuth(
                data.get("current_wind_direction", None)
            )

            temperature = self.smart_parse_float(
                data.get("current_temperature_celsius", None)
            )
            if temperature == 100:
                logging.error(
                    f"[{station.id}]: Temperature == 100: {temperature}. Dump: {json.dumps(data)}"
                )
            wind_speed = self.smart_parse_float(
                data.get("current_wind_speed_kph", None)
            )
            if wind_speed == 100:
                logging.error(
                    f"[{station.id}]: Wind speed == 100: {wind_speed}. Dump: {json.dumps(data)}"
                )

            max_wind_speed = self.smart_parse_float(
                data.get("daily_max_wind_speed", None)
            )
            if max_wind_speed == 100:
                logging.error(
                    f"[{station.id}]: Max wind speed == 100: {max_wind_speed}. Dump: {json.dumps(data)}"
                )

            cumulativeRain = self.smart_parse_float(
                data.get("total_daily_precipitation_at_record_timestamp", None)
            )
            if cumulativeRain == 100:
                logging.error(
                    f"[{station.id}]: Cumulative rain == 100: {cumulativeRain}. Dump: {json.dumps(data)}"
                )

            humidity = self.smart_parse_float(data.get("relative_humidity", None))
            if humidity == 100:
                logging.error(
                    f"[{station.id}]: Humidity == 100: {humidity}. Dump: {json.dumps(data)}"
                )

            pressure = self.smart_parse_float(data.get("pressure_hpa", None))
            if pressure == 100:
                logging.error(
                    f"[{station.id}]: Pressure == 100: {pressure}. Dump: {json.dumps(data)}"
                )

            maxTemp = self.smart_parse_float(data.get("daily_max_temperature", None))
            if maxTemp == 100:
                logging.error(
                    f"[{station.id}]: Max temperature == 100: {maxTemp}. Dump: {json.dumps(data)}"
                )

            minTemp = self.smart_parse_float(data.get("daily_min_temperature", None))

            if minTemp == 100:
                logging.error(
                    f"[{station.id}]: Min temperature == 100: {minTemp}. Dump: {json.dumps(data)}"
                )

            wr = WeatherRecord(
                id=None,
                station_id=station.id,
                source_timestamp=local_observation_time,
                temperature=temperature,
                wind_speed=wind_speed,
                max_wind_speed=max_wind_speed,
                wind_direction=wind_direction,
                rain=None,
                humidity=humidity,
                pressure=pressure,
                flagged=False,
                gatherer_thread_id=None,
                cumulative_rain=cumulativeRain,
                max_temperature=None,
                min_temperature=None,
                wind_gust=None,
                max_wind_gust=None,
            )

            return wr
        except KeyError as e:
            raise ValueError(f"Missing key {e} in data.")


    def fetch_data(self, station: WeatherStation, *args, **kwargs) -> dict:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }

        logging.info(f"Requesting {response.url}")
        response = requests.get(station.field1, headers=headers, timeout=5)

        if response.status_code != 200:
            raise Exception(f"Error: Received status code {response.status_code}")

        return {"live": response.text}


CODE_TO_NAME = {
    "VER": "version",
    "COD": "station_code",
    "SIG": "signature",
    "UPD": "record_timestamp",
    "TMP": "current_temperature_celsius",
    "WND": "current_wind_speed_kph",
    "AZI": "current_wind_direction",
    "BAR": "pressure_hpa",
    "HUM": "relative_humidity",
    "SUN": "solar_radiation_index",
    "UVI": "uva_index",
    "DHTM": "daily_max_temperature",
    "DLTM": "daily_min_temperature",
    "DHHM": "daily_max_humidity",
    "DLHM": "daily_min_humidity",
    "DHBR": "daily_max_pressure",
    "DLBR": "daily_min_pressure",
    "DGST": "daily_max_wind_speed",
    "DSUN": "daily_max_solar_radiation_index",
    "DHUV": "daily_max_uva_index",
    "DPCP": "total_daily_precipitation_at_record_timestamp",
    "WRUN": "wind_run_distance_daily",
    "MHTM": "monthly_max_temperature",
    "MLTM": "monthly_min_temperature",
    "MHHM": "monthly_max_humidity",
    "MLHM": "monthly_min_humidity",
    "MHBR": "monthly_max_pressure",
    "MLBR": "monthly_min_pressure",
    "MSUN": "monthly_max_solar_index",
    "MHUV": "monthly_max_uva_index",
    "MGST": "monthly_max_wind_speed",
    "MPCP": "total_precipitation_current_month",
    "YHTM": "yearly_max_temperature",
    "YLTM": "yearly_min_temperature",
    "YHHM": "yearly_max_humidity",
    "YLHM": "yearly_min_humidity",
    "YHBR": "yearly_max_pressure",
    "YLBR": "yearly_min_pressure",
    "YGST": "yearly_max_wind_speed",
    "YSUN": "yearly_max_solar_index",
    "YHUV": "yearly_max_uva_index",
    "YPCP": "total_precipitation_current_year",
}

WHITELIST = [
    "UPD",
    "TMP",
    "WND",
    "DGST",  # daily max wind speed: max_wind_speed
    "AZI",
    "DPCP",  # lluvia cumulativa
    "HUM",
    "BAR",
    "DHTM",
    "DLTM",
]
