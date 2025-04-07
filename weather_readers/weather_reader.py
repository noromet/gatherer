from abc import ABC, abstractmethod
import datetime
from schema import WeatherRecord, WeatherStation

class WeatherReader(ABC):
    def __init__(self, live_endpoint: str|None = None, historical_endpoint: str|None = None):
        self.live_endpoint = live_endpoint
        self.historical_endpoint = historical_endpoint

    @abstractmethod
    def parse(
            self,
            station: WeatherStation,
            live_data_response: str | None, 
            daily_data_response: str | None
    ) -> WeatherRecord:
        ...

    @abstractmethod
    def get_data(station: WeatherStation) -> WeatherRecord:
        ...

    @abstractmethod
    def get_live_endpoint() -> str:
        ...

    @abstractmethod
    def get_historical_endpoint() -> str:
        ...
    

    def assert_date_age(date: datetime.datetime) -> None:
        if date is None:
            raise ValueError("Date is None")
        
        if date.tzinfo is None:
            raise ValueError("Date has no timezone")
        
        if date.tzinfo != datetime.timezone.utc:
            raise ValueError("Date is not UTC")
        
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if (now_utc - date).total_seconds() > 1800:
            raise ValueError(f"Reading timestamp is too old to be stored as current. Observation time (UTC): {date}, current time (UTC): {now_utc}")