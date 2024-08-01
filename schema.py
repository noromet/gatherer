import datetime
import uuid

class WeatherStation:
    def __init__(self, id: uuid.uuid4, token: str):
        self.id = id
        self.token = token
        
class WeatherRecord:
    def __init__(self, id: uuid.uuid4, station_id: uuid.uuid4, timestamp: datetime.datetime, temperature: float, wind_speed: float, wind_direction: float, rain: float, humidity: float, pressure: float, flagged: bool):
        self.id = id
        self.station_id = station_id
        self.timestamp = timestamp
        self.temperature = temperature
        self.wind_speed = wind_speed
        self.wind_direction = wind_direction
        self.rain = rain
        self.humidity = humidity
        self.pressure = pressure
        self.flagged = flagged