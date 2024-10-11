from schema import WeatherRecord
from .utils import smart_parse_date, is_date_too_old, smart_parse_float, smart_azimuth
import requests

class MeteoclimaticReader:
    code_to_name_map = {
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
        "YPCP": "total_precipitation_current_year"
    }
    
    values_to_keep = [
        "UPD",
        "TMP",
        "WND",
        "DGST",
        "AZI",
        "DPCP", #lluvia cumulativa
        "HUM",
        "BAR"
    ]
        
        
    @staticmethod
    def parse(str_data: str) -> WeatherRecord:
        data = {}
        for line in str_data.strip().split("*"):
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, value = line.split("=")
            key = key.strip()
            value = value.strip()
            
            if key in MeteoclimaticReader.code_to_name_map.keys() and key in MeteoclimaticReader.values_to_keep:
                data[MeteoclimaticReader.code_to_name_map[key]] = value

        data["record_timestamp"] = smart_parse_date(data["record_timestamp"])
        if is_date_too_old(data["record_timestamp"]):
            raise ValueError("Record timestamp is too old to be stored as current.")
            
        try:
            return WeatherRecord(
                id=None,
                station_id=None,
                source_timestamp=data["record_timestamp"],
                temperature=smart_parse_float(data["current_temperature_celsius"]),
                wind_speed=smart_parse_float(data["current_wind_speed_kph"]),
                max_wind_speed=smart_parse_float(data["daily_max_wind_speed"]),
                wind_direction=smart_azimuth(data["current_wind_direction"]),
                rain=smart_parse_float(data["total_daily_precipitation_at_record_timestamp"]),
                humidity=smart_parse_float(data["relative_humidity"]),
                pressure=smart_parse_float(data["pressure_hpa"]),
                flagged=False
            )
        except KeyError as e:
            raise ValueError(f"Missing key {e} in data.")
    
    @staticmethod
    def curl_endpoint(endpoint: str) -> str:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        response = requests.get(endpoint, headers=headers, timeout=10)
        print(f"Requesting {response.url}")
        if response.status_code != 200:
            raise Exception(f"Error: Received status code {response.status_code}")
        return response.text
    
    @staticmethod
    def get_data(endpoint: str) -> dict:
        raw_data = MeteoclimaticReader.curl_endpoint(endpoint)
        return MeteoclimaticReader.parse(raw_data)
    
    