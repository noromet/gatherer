import os
from dotenv import load_dotenv
import concurrent.futures
import threading
import json
import argparse
import queue
from database import Database, database_connection
import weather_readers as api
from datetime import datetime
from uuid import uuid4
from logger import setup_logger, set_debug_mode
import logging
from time import tzset
from dataclasses import dataclass
from schema import WeatherStation

load_dotenv()

os.environ['TZ'] = 'UTC'
tzset()

# region config
@dataclass
class Config:
    DB_URL: str = os.getenv("DATABASE_CONNECTION_URL")
    MAX_THREADS: int = int(os.getenv("MAX_THREADS"))
    WEATHERLINK_V1_ENDPOINT: str = os.getenv("WEATHERLINK_V1_ENDPOINT")
    WEATHERLINK_V2_ENDPOINT: str = os.getenv("WEATHERLINK_V2_ENDPOINT")
    WUNDERGROUND_ENDPOINT: str = os.getenv("WUNDERGROUND_ENDPOINT")
    WUNDERGROUND_DAILY_ENDPOINT: str = os.getenv("WUNDERGROUND_DAILY_ENDPOINT")
    HOLFUY_LIVE_ENDPOINT: str = os.getenv("HOLFUY_LIVE_ENDPOINT")
    HOLFUY_HISTORIC_ENDPOINT: str = os.getenv("HOLFUY_HISTORIC_ENDPOINT")
    THINGSPEAK_ENDPOINT: str = os.getenv("THINGSPEAK_ENDPOINT")
    ECOWITT_ENDPOINT: str = os.getenv("ECOWITT_ENDPOINT")
    ECOWITT_DAILY_ENDPOINT: str = os.getenv("ECOWITT_DAILY_ENDPOINT")

config = Config()
# endregion

#region argument processing
def validate_args(args):
    conditions = [
        (args.all and args.type, "Cannot specify both --all and --type"),
        (args.all and args.id, "Cannot specify both --all and --id"),
        (args.type and args.id, "Cannot specify both --type and --id"),
        (not args.all and not args.type and not args.id, "Must specify --all, --type or --id")
    ]
    for condition, message in conditions:
        if condition:
            raise ValueError(message)
        
    return args
        
def get_args():
    parser = argparse.ArgumentParser(description="Weather Station Data Processor")
    parser.add_argument("--all", action="store_true", help="Read all stations")
    parser.add_argument("--type", type=str, help="Read stations with a specific type")
    parser.add_argument("--id", type=str, help="Read a single weather station by id")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run")
    parser.add_argument("--single-thread", action="store_true", help="Run in single-thread mode", default=False)
    return validate_args(parser.parse_args())
#endregion

# region processing
class Gatherer:
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
            logging.error(f"Station {station} already saved")
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
        valid_timezones = {'Europe/Madrid', 'Europe/Lisbon', 'Etc/UTC'}
        if station.data_timezone.key not in valid_timezones:
            logging.error(f"Invalid data timezone for station {station.id}. Skipping.")
            return False
        if station.local_timezone.key not in valid_timezones:
            logging.error(f"Invalid local timezone for station {station.id}. Skipping.")
            return False
        return True

    def process_station(self, station: WeatherStation) -> dict:
        """Processes a single station."""
        logging.info(f"Processing station {station.id}, type {station.connection_type}")
        if not self.validate_station_timezones(station):
            return {"status": "error", "error": f"Invalid timezone for station {station.id}"}

        reader = self.readers.get(station.connection_type)
        if not reader:
            message = f"Invalid connection type for station {station.id}"
            logging.error(message)
            return {"status": "error", "error": message}

        try:
            record = reader.get_data(station)
            if not record:
                message = f"No data retrieved for station {station.id}"
                logging.error(message)
                return {"status": "error", "error": message}

            self._process_record(record, station)
            return {"status": "success"}
        except Exception as e:
            logging.error(f"Error processing station {station.id}: {e}")
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
            logging.info(f"Record saved for station {station.id}")
        else:
            logging.debug(json.dumps(record.__dict__, indent=4, sort_keys=True, default=str))
            logging.info(f"Dry run enabled, record not saved for station {station.id}")

    def process_chunk(self, chunk: list[WeatherStation], chunk_number: int, result_queue: queue.Queue):
        """Processes a chunk of stations."""
        logging.info(f"Processing chunk {chunk_number} on {threading.current_thread().name}")
        results = {station.id: self.process_station(station) for station in chunk}
        result_queue.put(results)

    def multithread_processing(self, stations: list[WeatherStation]) -> dict:
        """Processes stations using multithreading."""
        chunks = self._split_into_chunks(stations)
        result_queue = queue.Queue()

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = [executor.submit(self.process_chunk, chunk, i, result_queue) for i, chunk in enumerate(chunks)]
            concurrent.futures.wait(futures)

        results = {}
        while not result_queue.empty():
            results.update(result_queue.get())
        return results

    def _split_into_chunks(self, stations: list[WeatherStation]) -> list[list[WeatherStation]]:
        """Splits stations into chunks for multithreading."""
        chunk_size = len(stations) // self.max_threads
        remainder_size = len(stations) % self.max_threads
        chunks = [stations[i * chunk_size:(i + 1) * chunk_size] for i in range(self.max_threads)]
        for i in range(remainder_size):
            chunks[i].append(stations[-(i + 1)])
        return chunks

    def process(self, single_thread: bool) -> dict:
        """Processes all stations."""
        if not self.stations:
            logging.error("No active stations found!")
            return {}

        if single_thread or len(self.stations) < 30:
            return {station.id: self.process_station(station) for station in self.stations}
        return self.multithread_processing(list(self.stations))
# endregion

# region main
def main():
    setup_logger()

    logging.info("Starting gatherer service.")
    
    args = get_args()

    dry_run = args.dry_run
    single_thread = args.single_thread
    run_id = uuid4().hex

    readers = {
        'meteoclimatic':    api.MeteoclimaticReader(),
        'weatherlink_v1':   api.WeatherLinkV1Reader(live_endpoint=config.WEATHERLINK_V1_ENDPOINT),
        'wunderground':     api.WundergroundReader(live_endpoint=config.WUNDERGROUND_ENDPOINT, daily_endpoint=config.WUNDERGROUND_DAILY_ENDPOINT),
        'weatherlink_v2':   api.WeatherlinkV2Reader(live_endpoint=config.WEATHERLINK_V2_ENDPOINT, daily_endpoint=config.WEATHERLINK_V2_ENDPOINT),
        'holfuy':           api.HolfuyReader(live_endpoint=config.HOLFUY_LIVE_ENDPOINT, daily_endpoint=config.HOLFUY_HISTORIC_ENDPOINT),
        'thingspeak':       api.ThingspeakReader(live_endpoint=config.THINGSPEAK_ENDPOINT),
        'ecowitt':          api.EcowittReader(live_endpoint=config.ECOWITT_ENDPOINT, daily_endpoint=config.ECOWITT_DAILY_ENDPOINT),
        'realtime':         api.RealtimeReader()
    }

    with database_connection(config.DB_URL):
        gatherer = Gatherer(run_id=run_id, dry_run=dry_run, max_threads=config.MAX_THREADS, readers=readers)
        
        if args.dry_run:
            logging.warning("[Dry run enabled]")
            set_debug_mode()
        else:
            logging.warning("[Dry run disabled]")
            Database.init_thread_record(run_id, datetime.now().replace(second=0, microsecond=0), command=" ".join(os.sys.argv))

        logging.info(f"Starting gatherer run {run_id}")

        if args.id:
            gatherer.add_station(Database.get_single_station(args.id))
        elif args.type:
            if args.type not in gatherer.readers.keys():
                raise ValueError(f"Invalid connection type: {args.type}. Available types are: {', '.join(gatherer.readers.keys())}")
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