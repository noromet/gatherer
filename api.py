import requests
import datetime
from schema import WeatherRecord
import json
from dateutil import parser

def smart_azimuth(azimuth) -> float:
    if type(azimuth) is not str:
        if type(azimuth) is int or type(azimuth) is float:
            if azimuth < 0 or azimuth > 360:
                raise ValueError(f"Invalid azimuth value: {azimuth}")
            return azimuth
        else:
            raise ValueError(f"Invalid azimuth value: {azimuth}")
        
    azimuth = azimuth.strip().lower().replace(" ", "").replace("º", "").replace("o", "w")
    
    translations = {
        "n": 0,
        "nne": 22.5,
        "ne": 45,
        "ene": 67.5,
        "e": 90,
        "ese": 112.5,
        "se": 135,
        "sse": 157.5,
        "s": 180,
        "ssw": 202.5,
        "sw": 225,
        "wsw": 247.5,
        "w": 270,
        "wnw": 292.5,
        "nw": 315,
        "nnw": 337.5
    }

    if azimuth in translations.keys():
        return translations[azimuth]
    else:
        try:
            return smart_parse_float(azimuth)
        except ValueError:
            raise ValueError(f"Invalid azimuth value: {azimuth}")

def smart_parse_date(date_str: str) -> datetime.datetime:
    try:
        return parser.parse(date_str)
    except ValueError as e:
        raise ValueError(f"Invalid date format: {e}")
    
def smart_parse_float(float_str: str) -> float:
    """
    Handles both comma and dot as decimal separator. Removes any non-numeric character other than the separator. Pray.
    """

    if not float_str:
        return 0.0
    
    if "," in float_str and "." in float_str:
        raise ValueError("Invalid float format: both comma and dot as separators.")
    
    if "," in float_str:
        float_str = float_str.replace(".", "").replace(",", ".")
    
    float_str = "".join([c for c in float_str if c.isdigit() or c == "."])
    
    return float(float_str)

def is_date_too_old(date: datetime.datetime) -> bool: #1hr
    now_minus_1_hour = (datetime.datetime.now(date.tzinfo) - datetime.timedelta(hours=1))
    return date < now_minus_1_hour

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
        "AZI",
        "DPCP",
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
                timestamp=data["record_timestamp"],
                temperature=smart_parse_float(data["current_temperature_celsius"]),
                wind_speed=smart_parse_float(data["current_wind_speed_kph"]),
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
        response = requests.get(endpoint, headers=headers)
        print(f"Requesting {response.url}")
        return response.text
    
    @staticmethod
    def get_data(endpoint: str) -> dict:
        raw_data = MeteoclimaticReader.curl_endpoint(endpoint)
        return MeteoclimaticReader.parse(raw_data)
    
    
class WeatherLinkV1Reader:
    @staticmethod
    def parse(str_data: str) -> WeatherRecord:
        try:
            data = json.loads(str_data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {e}. Check station connection parameters.")
        
        observation_time = datetime.datetime.strptime(data["observation_time_rfc822"], "%a, %d %b %Y %H:%M:%S %z")
        
        if is_date_too_old(observation_time):
            raise ValueError("Record timestamp is too old to be stored as current.")
        
        return WeatherRecord(
            id=None,
            station_id=None,
            timestamp=datetime.datetime.now().isoformat(),
            temperature=data["temp_c"],
            wind_speed=data["wind_mph"],
            wind_direction=data["wind_degrees"],
            rain=0.0,
            humidity=data["relative_humidity"],
            pressure=data["pressure_mb"],
            flagged = False
        )
    
    @staticmethod
    def curl_endpoint(endpoint: str, user_did: str, password: str, apiToken: str) -> str:
        response = requests.get(endpoint, {
            "user": user_did,
            "pass": password,
            "apiToken": apiToken
        })
        
        #print full url
        print(f"Requesting {response.url}")
        
        return response.text

    
    @staticmethod
    def get_data(endpoint: str, params: dict = {}) -> dict:
        assert params[0] is not None
        assert params[1] is not None
        assert params[2] is not None
        
        response = WeatherLinkV1Reader.curl_endpoint(endpoint, params[0], params[2], params[1])#did, password, apiToken are field1, field3, field2
        parsed = WeatherLinkV1Reader.parse(response)
        return parsed
    
class WeatherDotComReader:
    @staticmethod
    def parse(str_data: str) -> WeatherRecord:
        raise NotImplementedError()
        pass
    
    @staticmethod
    def curl_endpoint(endpoint: str, params: dict = {}) -> str:
        raise NotImplementedError()
        assert params["station_id"] is not None
        assert params["api_key"] is not None
        pass
    
    @staticmethod
    def get_data(endpoint: str, params: dict = {}) -> dict:
        raise NotImplementedError()
        assert params["station_id"] is not None
        assert params["api_key"] is not None
        pass