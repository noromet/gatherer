import psycopg2
from psycopg2 import pool
from psycopg2.extensions import connection as _connection
from psycopg2.extensions import cursor as _cursor
from psycopg2.extras import RealDictCursor
from typing import List, Tuple, Optional
import datetime
import uuid
import json
import logging

from schema import WeatherRecord, WeatherStation
from contextlib import contextmanager


class CursorFromConnectionFromPool:
    """Context manager for PostgreSQL cursor."""

    def __init__(self):
        self.connection: Optional[_connection] = None
        self.cursor: Optional[_cursor] = None

    def __enter__(self) -> _cursor:
        """Enter the context manager."""
        self.connection = Database.get_connection()
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        return self.cursor

    def __exit__(self, exception_type, exception_value, exception_traceback) -> None:
        """Exit the context manager."""
        if exception_value:
            self.connection.rollback()
        else:
            self.cursor.close()
            self.connection.commit()
        Database.return_connection(self.connection)


class Database:
    """Database class for managing PostgreSQL connections."""
    __connection_pool: Optional[pool.SimpleConnectionPool] = None

    STATION_FIELDS = [
        "id", "connection_type", "field1", "field2", "field3", 
        "pressure_offset", "data_timezone", "local_timezone"
    ]

    @classmethod
    def initialize(cls, connection_string: str) -> None:
        """Initialize the connection pool."""
        #if not local, ask for confirmation
        if ("localhost" not in connection_string) and ("127.0.0.1" not in connection_string):
            print("WARNING: CONNECTING TO A REMOTE DATABASE")
            if input("CONTINUE?: ").lower() != "y":
                print("Exiting...")
                exit()

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
                """
                INSERT INTO weather_record (
                    id, station_id, source_timestamp, taken_timestamp, temperature, 
                    wind_speed, max_wind_speed, wind_direction, rain, humidity, 
                    pressure, flagged, gatherer_thread_id, cumulative_rain, max_temperature, 
                    min_temperature, wind_gust, max_wind_gust
                ) 
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    record.id, record.station_id, record.source_timestamp, 
                    record.taken_timestamp, record.temperature, record.wind_speed, 
                    record.max_wind_speed, record.wind_direction, record.rain, 
                    record.humidity, record.pressure, record.flagged, 
                    record.gatherer_thread_id, record.cumulative_rain, record.max_temperature, 
                    record.min_temperature, record.wind_gust, record.max_wind_gust
                )
            )

    @classmethod
    def save_thread_record(cls, id: uuid, results: dict):
        if not results:
            logging.error("No results to save")
            return
        
        total_stations = len(results)
        error_stations = len([station for station in results.keys() if results[station]['status'] == 'error'])
        errors = {station: results[station]['error'] for station in results.keys() if results[station]['status'] == 'error'}

        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                """
                UPDATE gatherer_thread 
                SET total_stations = %s, error_stations = %s, errors = %s 
                WHERE id = %s
                """,
                (total_stations, error_stations, json.dumps(errors), id)
            )

    @classmethod
    def init_thread_record(cls, id: uuid, thread_timestamp: datetime.datetime, command: str):
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                """
                INSERT INTO gatherer_thread (id, thread_timestamp, command) 
                VALUES (%s, %s, %s)
                """,
                (id, thread_timestamp, command)
            )

    @staticmethod
    def get_all_stations() -> List[WeatherStation]:
        """Get all active weather stations."""
        query = f"""
        SELECT {', '.join(Database.STATION_FIELDS)} 
        FROM weather_station 
        WHERE status = 'active'
        AND connection_type != 'connection_disabled'
        """
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(query)
            stations = cursor.fetchall()
            return [WeatherStation(**station) for station in stations]
        
    def get_single_station(station_id: str) -> WeatherStation:
        """Get a single weather station by ID."""
        query = f"""
        SELECT {', '.join(Database.STATION_FIELDS)} 
        FROM weather_station 
        WHERE id = %s
        """
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(query, (station_id,))
            station = cursor.fetchone()
            return WeatherStation(**station) if station else None
        
    def get_stations_by_connection_type(station_type: str) -> List[WeatherStation]:
        """Get all weather stations by type."""
        query = f"""
        SELECT {', '.join(Database.STATION_FIELDS)} 
        FROM weather_station 
        WHERE connection_type = %s AND status = 'active'
        """
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(query, (station_type,))
            stations = cursor.fetchall()
            return [WeatherStation(**station) for station in stations]
        
    def increment_incident_count(station_id: str) -> None:
        """Increment the incident count for a weather station."""
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                """
                UPDATE weather_station 
                SET incident_count = COALESCE(incident_count, 0) + 1 
                WHERE id = %s
                """, 
                (station_id,)
            )

@contextmanager
def database_connection(db_url: str):
    """Context manager for database connection."""
    logging.info("Connecting to database...")
    Database.initialize(db_url)
    try:
        yield
    finally:
        Database.close_all_connections()
        logging.info("Database connections closed.")