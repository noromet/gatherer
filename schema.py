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
                 gathererRunId: uuid.uuid4, 
                 cumulativeRain: float,
                 maxTemp: float,
                 minTemp: float,
                 maxWindGust: float,
                 maxMaxWindGust: float):
        
        self.id = id
        self.station_id = station_id
        self.source_timestamp = source_timestamp
        self.temperature = temperature
        self.wind_speed = wind_speed
        self.max_wind_speed = max_wind_speed # today
        self.wind_direction = wind_direction
        self.rain = rain
        self.cumulativeRain = cumulativeRain
        self.humidity = humidity
        self.pressure = pressure
        self.flagged = flagged
        self.taken_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        self.gathererRunId = gathererRunId
        self.maxTemp = maxTemp # today
        self.minTemp = minTemp # today
        self.maxWindGust = maxWindGust
        self.maxMaxWindGust = maxMaxWindGust

    def sanity_check(self):
        temp_safe_range = (-39, 50)
        wind_safe_range = (0, 500)
        humidity_safe_range = (0, 100)
        pressure_safe_range = (800, 1100)

        if self.temperature:
            if not temp_safe_range[0] < self.temperature < temp_safe_range[1]:
                self.flagged = True
                self.temperature = None
        if self.maxTemp:
            if not temp_safe_range[0] < self.maxTemp < temp_safe_range[1]:
                self.flagged = True
                self.maxTemp = None
        if self.minTemp:
            if not temp_safe_range[0] < self.minTemp < temp_safe_range[1]:
                self.flagged = True
                self.minTemp = None

        if self.wind_speed:
            if not wind_safe_range[0] < self.wind_speed < wind_safe_range[1]:
                self.flagged = True
                self.wind_speed = None
        if self.max_wind_speed:
            if not wind_safe_range[0] < self.max_wind_speed < wind_safe_range[1]:
                self.flagged = True
                self.max_wind_speed = None
        if self.maxWindGust:
            if not wind_safe_range[0] < self.maxWindGust < wind_safe_range[1]:
                self.flagged = True
                self.maxWindGust = None
        if self.maxMaxWindGust:
            if not wind_safe_range[0] < self.maxMaxWindGust < wind_safe_range[1]:
                self.flagged = True
                self.maxMaxWindGust = None
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

        #implement further limits here

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
        if self.cumulativeRain is not None:
            self.cumulativeRain = round(self.cumulativeRain, decimals)
        if self.maxTemp is not None:
            self.maxTemp = round(self.maxTemp, decimals)
        if self.minTemp is not None:
            self.minTemp = round(self.minTemp, decimals)
        if self.maxWindGust is not None:
            self.maxWindGust = round(self.maxWindGust, decimals)
        if self.maxMaxWindGust is not None:
            self.maxMaxWindGust = round(self.maxMaxWindGust, decimals)


class GathererThread:
    def __init__(self, id: uuid.uuid4, timestamp: datetime.datetime, total_stations: int, error_stations: int, errors: dict, command: str):
        self.id = id
        self.timestamp = timestamp
        self.total_stations = total_stations
        self.error_stations = error_stations
        self.errors = errors
        self.command = command