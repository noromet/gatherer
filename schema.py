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
                 maxWindGust: float):
        
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
        self.taken_timestamp = datetime.datetime.now()
        self.gathererRunId = gathererRunId
        self.maxTemp = maxTemp # today
        self.minTemp = minTemp # today
        self.maxWindGust = maxWindGust

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

class GathererThread:
    def __init__(self, id: uuid.uuid4, timestamp: datetime.datetime, total_stations: int, error_stations: int, errors: dict, command: str):
        self.id = id
        self.timestamp = timestamp
        self.total_stations = total_stations
        self.error_stations = error_stations
        self.errors = errors
        self.command = command