from abc import ABC, abstractmethod
import datetime
from schema import WeatherRecord, WeatherStation

class WeatherReader(ABC):
    def __init__():
        ...

    @abstractmethod
    def parse() -> WeatherRecord:
        ...

    @abstractmethod
    def get_data() -> WeatherRecord:
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