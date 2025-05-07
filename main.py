"""
main.py

Entry point for the gatherer module.
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

from time import tzset
from uuid import uuid4
from datetime import datetime
import logging
from dataclasses import dataclass
import argparse

from dotenv import load_dotenv

import gatherer.weather_readers as api
from gatherer.database import database_connection, Database
from gatherer.logger import (
    set_debug_mode,
    setup_logger,
)

from gatherer.gatherer import Gatherer

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
    parser.add_argument(
        "--test-run",
        action="store_true",
        help="Perform a test run: no database save and benchmarking",
    )
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

    dry_run = args.test_run
    benchmark = args.test_run

    single_thread = args.single_thread
    run_id = uuid4().hex

    # Create reader factory functions instead of instances
    readers = {
        "meteoclimatic": lambda: api.MeteoclimaticReader(is_benchmarking=benchmark),
        "weatherlink_v1": lambda: api.WeatherlinkV1Reader(
            live_endpoint=config.weatherlink_v1_endpoint, is_benchmarking=benchmark
        ),
        "wunderground": lambda: api.WundergroundReader(
            live_endpoint=config.wunderground_endpoint,
            daily_endpoint=config.wunderground_daily_endpoint,
            is_benchmarking=benchmark,
        ),
        "weatherlink_v2": lambda: api.WeatherlinkV2Reader(
            live_endpoint=config.weatherlink_v2_endpoint,
            daily_endpoint=config.weatherlink_v2_endpoint,
            is_benchmarking=benchmark,
        ),
        "holfuy": lambda: api.HolfuyReader(
            live_endpoint=config.holfuy_live_endpoint,
            daily_endpoint=config.holfuy_daily_endpoint,
            is_benchmarking=benchmark,
        ),
        "thingspeak": lambda: api.ThingspeakReader(
            live_endpoint=config.thingspeak_endpoint, is_benchmarking=benchmark
        ),
        "ecowitt": lambda: api.EcowittReader(
            live_endpoint=config.cowitt_endpoint,
            daily_endpoint=config.ecowitt_daily_endpoint,
            is_benchmarking=benchmark,
        ),
        "realtime": lambda: api.RealtimeReader(is_benchmarking=benchmark),
    }

    with database_connection(config.db_url):
        gatherer = Gatherer(
            run_id=run_id,
            dry_run=dry_run,
            max_threads=config.max_threads,
            readers=readers,
        )

        if dry_run:
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

        if not dry_run:
            logging.info("Saving thread record")
            Database.save_thread_record(run_id, results)


if __name__ == "__main__":
    main()

# endregion
