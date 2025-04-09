"""
This module provides a PostgreSQL database management interface for a
weather data gathering application.

It includes:
- A `Database` class for managing database connections, executing queries, and
    interacting with weather-related tables.
- A `CursorFromConnectionFromPool` context manager for safely handling database cursors.
- Utility methods for saving weather records, managing gatherer thread records,
    and retrieving weather station data.
- A `database_connection` context manager for initializing and closing database connections.

Key Features:
- Connection pooling using `psycopg2.pool.SimpleConnectionPool` for efficient database access.
- Support for saving and updating weather records and gatherer thread metadata.
- Methods for querying weather stations based on status, connection type, or specific IDs.
- Automatic handling of database transactions and error rollback.

Dependencies:
- `psycopg2` for PostgreSQL interaction.
- `schema` module for `WeatherRecord` and `WeatherStation` data models.
- Standard libraries: `datetime`, `uuid`, `json`, `logging`, and `contextlib`.

This module is designed to ensure safe and efficient database operations for the
weather data gathering system.
"""

from typing import List, Optional
import datetime
import uuid
import json
import logging
import sys
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool
from psycopg2.extensions import connection as _connection
from psycopg2.extensions import cursor as _cursor
from psycopg2.extras import RealDictCursor

from schema import WeatherRecord, WeatherStation


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
        "id",
        "connection_type",
        "field1",
        "field2",
        "field3",
        "pressure_offset",
        "data_timezone",
        "local_timezone",
    ]

    @classmethod
    def initialize(cls, connection_string: str) -> None:
        """Initialize the connection pool."""
        # if not local, ask for confirmation
        if ("localhost" not in connection_string) and (
            "127.0.0.1" not in connection_string
        ):
            print("WARNING: CONNECTING TO A REMOTE DATABASE")
            if input("CONTINUE?: ").lower() != "y":
                print("Exiting...")
                sys.exit()

        cls.__connection_pool = pool.SimpleConnectionPool(1, 10, dsn=connection_string)

    @classmethod
    def get_connection(cls) -> _connection:
        """Get a connection from the pool."""
        if cls.__connection_pool is None:
            raise psycopg2.OperationalError("Connection pool is not initialized.")
        conn = cls.__connection_pool.getconn()
        conn.set_client_encoding("utf8")
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
                    record.id,
                    record.station_id,
                    record.source_timestamp,
                    record.taken_timestamp,
                    record.temperature,
                    record.wind_speed,
                    record.max_wind_speed,
                    record.wind_direction,
                    record.rain,
                    record.humidity,
                    record.pressure,
                    record.flagged,
                    record.gatherer_thread_id,
                    record.cumulative_rain,
                    record.max_temperature,
                    record.min_temperature,
                    record.wind_gust,
                    record.max_wind_gust,
                ),
            )

    @classmethod
    def save_thread_record(cls, thread_record_id: uuid, results: dict):
        """
        Save the results of a gatherer thread to the database.

        This method updates the `gatherer_thread` table with the total number of stations,
        the number of stations with errors, and the error details for a specific thread.

        Args:
            thread_record_id (uuid): The unique identifier of the gatherer thread.
            results (dict): A dictionary containing the results of the gatherer thread,
                            where each key is a station ID and the value is a dictionary
                            with the status and error details (if any).

        Returns:
            None
        """
        if not results:
            logging.error("No results to save")
            return

        total_stations = len(results)
        error_stations = len(
            [
                station
                for station in results.keys()
                if results[station]["status"] == "error"
            ]
        )
        errors = {
            station: results[station]["error"]
            for station in results.keys()
            if results[station]["status"] == "error"
        }

        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                """
                UPDATE gatherer_thread 
                SET total_stations = %s, error_stations = %s, errors = %s 
                WHERE id = %s
                """,
                (total_stations, error_stations, json.dumps(errors), thread_record_id),
            )

    @classmethod
    def init_thread_record(
        cls, thread_record_id: uuid, thread_timestamp: datetime.datetime, command: str
    ):
        """
        Initialize a new gatherer thread record in the database.
        This method inserts a new record into the `gatherer_thread` table with the
        specified ID, timestamp, and command.
        Args:
            thread_record_id (uuid): The unique identifier of the gatherer thread.
            thread_timestamp (datetime.datetime): The timestamp of the thread.
            command (str): The command that initiated the thread.
        Returns:
            None
        """
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                """
                INSERT INTO gatherer_thread (thread_record_id, thread_timestamp, command) 
                VALUES (%s, %s, %s)
                """,
                (thread_record_id, thread_timestamp, command),
            )

    @classmethod
    def get_all_stations(cls) -> List[WeatherStation]:
        """
        Fetches all active weather stations from the database.
        Returns:
            List[WeatherStation]: A list of WeatherStation objects representing active stations.
        """
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

    @classmethod
    def get_single_station(cls, station_id: str) -> WeatherStation:
        """
        Fetch a single weather station by ID.
        Returns:
            WeatherStation: A WeatherStation object representing the station.
        Raises:
            ValueError: If the station ID is not found.
        """
        query = f"""
        SELECT {', '.join(Database.STATION_FIELDS)}
        FROM weather_station 
        WHERE id = %s
        """
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(query, (station_id,))
            station = cursor.fetchone()
            return WeatherStation(**station) if station else None

    @classmethod
    def get_stations_by_connection_type(cls, station_type: str) -> List[WeatherStation]:
        """
        Fetch weather stations by connection type.
        Args:
            station_type (str): The connection type of the stations to fetch.
        Returns:
            List[WeatherStation]: A list of WeatherStation objects representing the stations.
        """
        query = f"""
        SELECT {', '.join(Database.STATION_FIELDS)}
        FROM weather_station 
        WHERE connection_type = %s AND status = 'active'
        """
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(query, (station_type,))
            stations = cursor.fetchall()
            return [WeatherStation(**station) for station in stations]

    @classmethod
    def increment_incident_count(cls, station_id: str) -> None:
        """
        Increment the incident count for a weather station.
        Args:
            station_id (str): The ID of the weather station.
        Returns:
            None
        """
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute(
                """
                UPDATE weather_station 
                SET incident_count = COALESCE(incident_count, 0) + 1 
                WHERE id = %s
                """,
                (station_id,),
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
