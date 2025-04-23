"""
main.py

This module serves as the entry point for the gatherer module.
It is responsible for gathering, preprocessing, and saving weather
station data from various sources using multithreading or
single-threaded execution. The application supports
multiple connection types and allows for flexible configuration through environment
variables and command-line arguments.

Key Features:
- Multithreaded or single-threaded data processing.
- Support for multiple weather station connection types.
- Validation of station timezones and data integrity.
- Dry-run mode for testing without saving data to the database.
- Configurable via environment variables and command-line arguments.

Dependencies:
- dotenv for loading environment variables.
- argparse for command-line argument parsing.
- schema, logger, weather_readers, and database modules for core functionality.
- psycopg2 for PostgreSQL database connection.

Usage:
Run the script with appropriate command-line arguments to
process weather station data.
"""

import os
import concurrent.futures
import threading
import json
import queue
from time import tzset
from uuid import uuid4
from datetime import datetime
import logging
from dataclasses import dataclass
import argparse

from dotenv import load_dotenv

from schema import WeatherStation
from logger import setup_logger, set_debug_mode
import weather_readers as api
from database import Database, database_connection

load_dotenv()

os.environ["TZ"] = "UTC"
tzset()


# region config
@dataclass
class Config:
    """
    Configuration class for storing application settings.

    Attributes:
        db_url (str): Database connection URL.
        max_threads (int): Maximum number of threads for multithreading.
        weatherlink_v1_endpoint (str): Endpoint for WeatherLink V1 API.
        weatherlink_v2_endpoint (str): Endpoint for WeatherLink V2 API.
        wunderground_endpoint (str): Endpoint for Wunderground API.
        wunderground_daily_endpoint (str): Endpoint for Wunderground daily data.
        holfuy_live_endpoint (str): Endpoint for Holfuy live data.
        holfuy_daily_endpoint (str): Endpoint for Holfuy historical data.
        thingspeak_endpoint (str): Endpoint for ThingSpeak API.
        cowitt_endpoint (str): Endpoint for Ecowitt API.
        ecowitt_daily_endpoint (str): Endpoint for Ecowitt daily data.
    """

    db_url: str = os.getenv("DATABASE_CONNECTION_URL")
    max_threads: int = int(os.getenv("MAX_THREADS"))
    weatherlink_v1_endpoint: str = os.getenv("WEATHERLINK_V1_ENDPOINT")
    weatherlink_v2_endpoint: str = os.getenv("WEATHERLINK_V2_ENDPOINT")
    wunderground_endpoint: str = os.getenv("WUNDERGROUND_ENDPOINT")
    wunderground_daily_endpoint: str = os.getenv("WUNDERGROUND_DAILY_ENDPOINT")
    holfuy_live_endpoint: str = os.getenv("HOLFUY_LIVE_ENDPOINT")
    holfuy_daily_endpoint: str = os.getenv("HOLFUY_HISTORIC_ENDPOINT")
    thingspeak_endpoint: str = os.getenv("THINGSPEAK_ENDPOINT")
    cowitt_endpoint: str = os.getenv("ECOWITT_ENDPOINT")
    ecowitt_daily_endpoint: str = os.getenv("ECOWITT_DAILY_ENDPOINT")


config = Config()
# endregion


# region argument processing


def get_args() -> argparse.Namespace:
    """
    Parses command-line arguments.
    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """

    parser = argparse.ArgumentParser(description="Weather Station Data Processor")
    parser.add_argument("--all", action="store_true", help="Read all stations")
    parser.add_argument("--type", type=str, help="Read stations with a specific type")
    parser.add_argument("--id", type=str, help="Read a single weather station by id")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run")
    parser.add_argument(
        "--single-thread",
        action="store_true",
        help="Run in single-thread mode",
        default=False,
    )
    args = parser.parse_args()

    conditions = [
        (args.all and args.type, "Cannot specify both --all and --type"),
        (args.all and args.id, "Cannot specify both --all and --id"),
        (args.type and args.id, "Cannot specify both --type and --id"),
        (
            not args.all and not args.type and not args.id,
            "Must specify --all, --type or --id",
        ),
    ]
    for condition, message in conditions:
        if condition:
            raise ValueError(message)

    return args


# endregion


# region processing
class Gatherer:
    """
    Main class for gathering and processing weather station data.

    Attributes:
        run_id (str): Unique identifier for the current run.
        dry_run (bool): Indicates whether the application is in dry-run mode.
        max_threads (int): Maximum number of threads for multithreading.
        stations (set): Set of weather stations to process.
        readers (dict): Dictionary of data readers by connection type.

    Methods:
        add_station(station): Adds a single weather station to the set.
        add_many(stations): Adds multiple weather stations to the set.
        validate_station_timezones(station): Validates the timezones of a station.
        process_station(station): Processes a single weather station.
        _process_record(record, station): Processes and saves a data record.
        process_chunk(chunk, chunk_number, result_queue): Processes a chunk of stations.
        multithread_processing(stations): Processes stations using multithreading.
        _split_into_chunks(stations): Splits stations into chunks for multithreading.
        process(single_thread): Processes all stations.
    """

    def __init__(self, run_id: str, dry_run: bool, max_threads: int, readers: dict):
        """
        Initializes the Gatherer instance.

        :param run_id: Unique identifier for the run.
        :param dry_run: Whether to perform a dry run.
        :param max_threads: Maximum number of threads for multithreading.
        :param readers: Dictionary of data readers by connection type.
        """
        self.run_id = run_id
        self.dry_run = dry_run
        self.max_threads = max_threads
        self.stations = set()
        self.readers = readers

    def add_station(self, station: WeatherStation):
        """Adds a single station to the set."""
        if not station:
            logging.error("Station is None")
            return
        if station in self.stations:
            logging.error("Station %s already saved", station)
            return
        self.stations.add(station)

    def add_many(self, stations: list[WeatherStation]):
        """Adds multiple stations to the set."""
        if not stations:
            logging.error("Stations list is None")
            return
        for station in stations:
            self.add_station(station)

    def validate_station_timezones(self, station: WeatherStation) -> bool:
        """Validates the timezones of a station."""
        valid_timezones = {"Europe/Madrid", "Europe/Lisbon", "Etc/UTC"}
        if station.data_timezone.key not in valid_timezones:
            logging.error("Invalid data timezone for station %s. Skipping.", station.id)
            return False
        if station.local_timezone.key not in valid_timezones:
            logging.error(
                "Invalid local timezone for station %s. Skipping.", station.id
            )
            return False
        return True

    def process_station(self, station: WeatherStation) -> dict:
        """Processes a single station."""
        logging.info(
            "Processing station %s, type %s", station.id, station.connection_type
        )
        if not self.validate_station_timezones(station):
            return {
                "status": "error",
                "error": f"Invalid timezone for station {station.id}",
            }

        reader = self.readers.get(station.connection_type)
        if not reader:
            message = f"Invalid connection type for station {station.id}"
            logging.error(message)
            return {"status": "error", "error": message}

        try:
            record = reader.read(station)
            if not record:
                message = f"No data retrieved for station {station.id}"
                logging.error(message)
                return {"status": "error", "error": message}

            self._process_record(record, station)
            return {"status": "success"}
        except Exception as e:
            logging.error("Error processing station %s: %s", station.id, e)
            if not self.dry_run:
                Database.increment_incident_count(station.id)
            return {"status": "error", "error": str(e)}

    def _process_record(self, record, station: WeatherStation):
        """Processes and saves a record."""
        record.station_id = station.id
        record.gatherer_thread_id = self.run_id
        record.sanity_check()
        record.apply_pressure_offset(station.pressure_offset)
        record.apply_rounding(1)

        if not self.dry_run:
            Database.save_record(record)
            logging.info("Record saved for station %s", station.id)
        else:
            logging.debug(
                json.dumps(record.__dict__, indent=4, sort_keys=True, default=str)
            )
            logging.info("Dry run enabled, record not saved for station %s", station.id)

    def process_chunk(
        self, chunk: list[WeatherStation], chunk_number: int, result_queue: queue.Queue
    ):
        """Processes a chunk of stations."""
        logging.info(
            "Processing chunk %d on %s", chunk_number, threading.current_thread().name
        )
        results = {station.id: self.process_station(station) for station in chunk}
        result_queue.put(results)

    def multithread_processing(self, stations: list[WeatherStation]) -> dict:
        """Processes stations using multithreading."""
        chunks = self._split_into_chunks(stations)
        result_queue = queue.Queue()

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_threads
        ) as executor:
            futures = [
                executor.submit(self.process_chunk, chunk, i, result_queue)
                for i, chunk in enumerate(chunks)
            ]
            concurrent.futures.wait(futures)

        results = {}
        while not result_queue.empty():
            results.update(result_queue.get())
        return results

    def _split_into_chunks(
        self, stations: list[WeatherStation]
    ) -> list[list[WeatherStation]]:
        """Splits stations into chunks for multithreading."""
        chunk_size = len(stations) // self.max_threads
        remainder_size = len(stations) % self.max_threads
        chunks = [
            stations[i * chunk_size : (i + 1) * chunk_size]
            for i in range(self.max_threads)
        ]
        for i in range(remainder_size):
            chunks[i].append(stations[-(i + 1)])
        return chunks

    def process(self, single_thread: bool) -> dict:
        """Processes all stations."""
        if not self.stations:
            logging.error("No active stations found!")
            return {}

        if single_thread or len(self.stations) < 30:
            return {
                station.id: self.process_station(station) for station in self.stations
            }
        return self.multithread_processing(list(self.stations))


# endregion


# region main
def main():
    """
    Main function to run the gatherer service.
    It sets up the logger, parses command-line arguments,
    initializes the Gatherer class, and processes weather stations.
    It also handles dry-run mode and saves thread records to the database.

    Raises:
        ValueError: If invalid command-line arguments are provided.
    """
    setup_logger()

    logging.info("Starting gatherer service.")

    args = get_args()

    dry_run = args.dry_run
    single_thread = args.single_thread
    run_id = uuid4().hex

    readers = {
        "meteoclimatic": api.MeteoclimaticReader(),
        "weatherlink_v1": api.WeatherlinkV1Reader(
            live_endpoint=config.weatherlink_v1_endpoint
        ),
        "wunderground": api.WundergroundReader(
            live_endpoint=config.wunderground_endpoint,
            daily_endpoint=config.wunderground_daily_endpoint,
        ),
        "weatherlink_v2": api.WeatherlinkV2Reader(
            live_endpoint=config.weatherlink_v2_endpoint,
            daily_endpoint=config.weatherlink_v2_endpoint,
        ),
        "holfuy": api.HolfuyReader(
            live_endpoint=config.holfuy_live_endpoint,
            daily_endpoint=config.holfuy_daily_endpoint,
        ),
        "thingspeak": api.ThingspeakReader(live_endpoint=config.thingspeak_endpoint),
        "ecowitt": api.EcowittReader(
            live_endpoint=config.cowitt_endpoint,
            daily_endpoint=config.ecowitt_daily_endpoint,
        ),
        "realtime": api.RealtimeReader(),
    }

    with database_connection(config.db_url):
        gatherer = Gatherer(
            run_id=run_id,
            dry_run=dry_run,
            max_threads=config.max_threads,
            readers=readers,
        )

        if args.dry_run:
            logging.warning("[Dry run enabled]")
            set_debug_mode()
        else:
            logging.warning("[Dry run disabled]")
            Database.init_thread_record(
                run_id,
                datetime.now().replace(second=0, microsecond=0),
                command=" ".join(os.sys.argv),
            )

        logging.info("Starting gatherer run %s", run_id)

        if args.id:
            gatherer.add_station(Database.get_single_station(args.id))
        elif args.type:
            if args.type not in gatherer.readers.keys():
                raise ValueError(
                    f"Invalid connection type: {args.type}. Available "
                    + f"types are: {', '.join(gatherer.readers.keys())}"
                )
            gatherer.add_many(Database.get_stations_by_connection_type(args.type))
        else:
            gatherer.add_many(Database.get_all_stations())

        results = gatherer.process(single_thread)

        if not args.dry_run:
            logging.info("Saving thread record")
            Database.save_thread_record(run_id, results)


if __name__ == "__main__":
    main()

# endregion
