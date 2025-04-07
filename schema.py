import datetime
import uuid

class WeatherRecord:
    def __init__(self, 
                 id: uuid.uuid4, 
                 station_id: uuid.uuid4, 
                 source_timestamp: datetime.datetime, 
                 temperature: float, 
                 wind_speed: float, 
                 max_wind_speed: float, 
                 wind_direction: float, 
                 rain: float, 
                 humidity: float, 
                 pressure: float, 
                 flagged: bool, 
                 gatherer_thread_id: uuid.uuid4, 
                 cumulative_rain: float,
                 max_temperature: float,
                 min_temperature: float,
                 wind_gust: float,
                 max_wind_gust: float):
        
        self.id = id
        self.station_id = station_id
        self.source_timestamp = source_timestamp
        self.temperature = temperature
        self.wind_speed = wind_speed
        self.max_wind_speed = max_wind_speed  # today
        self.wind_direction = wind_direction
        self.rain = rain
        self.cumulative_rain = cumulative_rain
        self.humidity = humidity
        self.pressure = pressure
        self.flagged = flagged
        self.taken_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        self.gatherer_thread_id = gatherer_thread_id
        self.max_temperature = max_temperature  # today
        self.min_temperature = min_temperature  # today
        self.wind_gust = wind_gust
        self.max_wind_gust = max_wind_gust

    def sanity_check(self):
        temp_safe_range = (-39, 50)
        wind_safe_range = (0, 500)
        humidity_safe_range = (0, 100)
        pressure_safe_range = (800, 1100)

        if self.temperature:
            if not temp_safe_range[0] < self.temperature < temp_safe_range[1]:
                self.flagged = True
                self.temperature = None
        if self.max_temperature:
            if not temp_safe_range[0] < self.max_temperature < temp_safe_range[1]:
                self.flagged = True
                self.max_temperature = None
        if self.min_temperature:
            if not temp_safe_range[0] < self.min_temperature < temp_safe_range[1]:
                self.flagged = True
                self.min_temperature = None

        if self.wind_speed:
            if not wind_safe_range[0] < self.wind_speed < wind_safe_range[1]:
                self.flagged = True
                self.wind_speed = None
        if self.max_wind_speed:
            if not wind_safe_range[0] < self.max_wind_speed < wind_safe_range[1]:
                self.flagged = True
                self.max_wind_speed = None
        if self.wind_gust:
            if not wind_safe_range[0] < self.wind_gust < wind_safe_range[1]:
                self.flagged = True
                self.wind_gust = None
        if self.max_wind_gust:
            if not wind_safe_range[0] < self.max_wind_gust < wind_safe_range[1]:
                self.flagged = True
                self.max_wind_gust = None
        if self.wind_direction:
            if not 0 <= self.wind_direction <= 360:
                self.flagged = True
                self.wind_direction = None

        if self.humidity:
            if not humidity_safe_range[0] < self.humidity < humidity_safe_range[1]:
                self.flagged = True
                self.humidity = None

        if self.pressure:
            if not pressure_safe_range[0] < self.pressure < pressure_safe_range[1]:
                self.flagged = True
                self.pressure = None

        # Implement further limits here

    def apply_pressure_offset(self, offset: float):
        if offset:
            self.pressure += offset

    def apply_rounding(self, decimals: int = 1):
        if self.temperature is not None:
            self.temperature = round(self.temperature, decimals)
        if self.wind_speed is not None:
            self.wind_speed = round(self.wind_speed, decimals)
        if self.max_wind_speed is not None:
            self.max_wind_speed = round(self.max_wind_speed, decimals)
        if self.humidity is not None:
            self.humidity = round(self.humidity, decimals)
        if self.pressure is not None:
            self.pressure = round(self.pressure, decimals)
        if self.rain is not None:
            self.rain = round(self.rain, decimals)
        if self.cumulative_rain is not None:
            self.cumulative_rain = round(self.cumulative_rain, decimals)
        if self.max_temperature is not None:
            self.max_temperature = round(self.max_temperature, decimals)
        if self.min_temperature is not None:
            self.min_temperature = round(self.min_temperature, decimals)
        if self.wind_gust is not None:
            self.wind_gust = round(self.wind_gust, decimals)
        if self.max_wind_gust is not None:
            self.max_wind_gust = round(self.max_wind_gust, decimals)


class GathererThread:
    def __init__(self, 
        id: uuid.uuid4, 
        thread_timestamp: datetime.datetime, 
        total_stations: int, 
        error_stations: int, 
        errors: dict, 
        command: str
    ):
        self.id = id
        self.thread_timestamp = thread_timestamp
        self.total_stations = total_stations
        self.error_stations = error_stations
        self.errors = errors
        self.command = command


class WeatherStation:
    def __init__(self,
        id: uuid.uuid4,
        connection_type: str,
        field1: str,
        field2: str,
        field3: str,
        pressure_offset: float,
        data_timezone: datetime.tzinfo,
        local_timezone: datetime.tzinfo
    ):
        self.id = id
        self.connection_type = connection_type
        self.field1 = field1
        self.field2 = field2
        self.field3 = field3
        self.pressure_offset = pressure_offset
        self.data_timezone = data_timezone
        self.local_timezone = local_timezone