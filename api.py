import requests
import datetime
from schema import WeatherRecord

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
        
        
    @staticmethod
    def parse(str_data: str) -> WeatherRecord:
        data = {}
        for line in str_data.split("\n"):
            line = line.strip()
            if line.startswith("*") and not line.endswith("*"):
                key, value = line.split("=")
                key = key.strip()
                value = value.strip()
                
                if key == "*UPD":
                    value = datetime.datetime.now().isoformat()
                if key not in ["*VER", "*COD", "*SIG", "*UPD"]:
                    value = float(value)
                                      
                data[MeteoclimaticReader.code_to_name_map[key.strip()[1:]]] = value
            
        return WeatherRecord(
            id=None,
            station_id=None,
            timestamp=data["record_timestamp"],
            temperature=data["current_temperature_celsius"],
            wind_speed=data["current_wind_speed_kph"],
            wind_direction=data["current_wind_direction"],
            rain=data["total_daily_precipitation_at_record_timestamp"],
            humidity=data["relative_humidity"],
            pressure=data["pressure_hpa"],
            flagged=False
        )
    
    @staticmethod
    def curl_endpoint(endpoint: str) -> str:
        response = requests.get(endpoint)
        return response.text
    
    @staticmethod
    def get_data(endpoint: str) -> dict:
        raw_data = MeteoclimaticReader.curl_endpoint(endpoint)
        return MeteoclimaticReader.parse(raw_data)