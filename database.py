import psycopg2
from psycopg2 import pool
from psycopg2.extensions import connection as _connection
from psycopg2.extensions import cursor as _cursor
from typing import List, Tuple, Optional
import uuid

from schema import WeatherRecord

class Database:
    """Database class for managing PostgreSQL connections."""
    __connection_pool: Optional[pool.SimpleConnectionPool] = None

    @classmethod
    def initialize(cls, connection_string: str) -> None:
        """Initialize the connection pool."""
        cls.__connection_pool = pool.SimpleConnectionPool(1, 10, dsn=connection_string)

    @classmethod
    def get_connection(cls) -> _connection:
        """Get a connection from the pool."""
        if cls.__connection_pool is None:
            raise psycopg2.OperationalError("Connection pool is not initialized.")
        conn = cls.__connection_pool.getconn()
        conn.set_client_encoding('utf8')
        return conn

    @classmethod
    def return_connection(cls, connection: _connection) -> None:
        """Return a connection to the pool."""
        cls.__connection_pool.putconn(connection)

    @classmethod
    def close_all_connections(cls) -> None:
        """Close all connections in the pool."""
        cls.__connection_pool.closeall()
        
    @classmethod
    def save_record(cls, record: WeatherRecord) -> None:
        """Save a weather record to the database."""
        record.id = str(uuid.uuid4())
        
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                "INSERT INTO weather_record (id, station_id, source_timestamp, taken_timestamp, temperature, wind_speed, max_wind_speed, wind_direction, rain, humidity, pressure, flagged) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (record.id, record.station_id, record.source_timestamp, record.taken_timestamp, record.temperature, record.wind_speed, record.max_wind_speed, record.wind_direction, record.rain, record.humidity, record.pressure, record.flagged)
            )


class CursorFromConnectionFromPool:
    """Context manager for PostgreSQL cursor."""

    def __init__(self):
        self.connection: Optional[_connection] = None
        self.cursor: Optional[_cursor] = None

    def __enter__(self) -> _cursor:
        """Enter the context manager."""
        self.connection = Database.get_connection()
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exception_type, exception_value, exception_traceback) -> None:
        """Exit the context manager."""
        if exception_value:
            self.connection.rollback()
        else:
            self.cursor.close()
            self.connection.commit()
        Database.return_connection(self.connection)


def get_all_stations() -> List[Tuple]:
    """Get all active weather stations."""
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("SELECT id, connection_type, field1, field2, field3 FROM weather_station WHERE status = 'active'")
        stations = cursor.fetchall()
        return stations
    
def get_single_station(station_id: str) -> Tuple:
    """Get a single weather station by ID."""
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("SELECT id, connection_type, field1, field2, field3 FROM weather_station WHERE id = %s", (station_id,))
        station = cursor.fetchone()
        return station
    
def get_stations_by_type(station_type: str) -> List[Tuple]:
    """Get all weather stations by type."""
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("SELECT id, connection_type, field1, field2, field3 FROM weather_station WHERE connection_type = %s AND status = 'active'", (station_type,))
        stations = cursor.fetchall()
        return stations
    
def increment_incident_count(station_id: str) -> None:
    """Increment the incident count for a weather station."""
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute(
            "UPDATE weather_station SET incident_count = COALESCE(incident_count, 0) + 1 WHERE id = %s", 
            (station_id,)
        )