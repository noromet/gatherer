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
                 windGust: float):
        
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
        self.windGust = windGust

    def sanity_check(self):
        if not -39 < float(self.temperature) < 50:
            self.temperature = None
            self.flagged = True
        if not -39 < float(self.maxTemp) < 50:
            self.maxTemp = None
            self.flagged = True
        if not -39 < float(self.minTemp) < 50:
            self.minTemp = None
            self.flagged = True

        #implement further limits here

class GathererThread:
    def __init__(self, id: uuid.uuid4, timestamp: datetime.datetime, total_stations: int, error_stations: int, errors: dict, command: str):
        self.id = id
        self.timestamp = timestamp
        self.total_stations = total_stations
        self.error_stations = error_stations
        self.errors = errors
        self.command = command