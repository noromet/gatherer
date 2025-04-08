import os
from dotenv import load_dotenv
import concurrent.futures
import threading
import json
import argparse
import queue
from database import Database, get_all_stations, get_single_station, get_stations_by_connection_type, increment_incident_count
import weather_readers as api
from datetime import datetime
from uuid import uuid4
from logger import setup_logger, set_debug_mode
import logging
from time import tzset
from zoneinfo import ZoneInfo
from schema import WeatherStation

os.environ['TZ'] = 'UTC'
tzset()

# region definitions
load_dotenv(verbose=True)
DB_URL = os.getenv("DATABASE_CONNECTION_URL")
MAX_THREADS = int(os.getenv("MAX_THREADS"))
WEATHERLINK_V1_ENDPOINT = os.getenv("WEATHERLINK_V1_ENDPOINT")
WEATHERLINK_V2_ENDPOINT = os.getenv("WEATHERLINK_V2_ENDPOINT")
WUNDERGROUND_ENDPOINT = os.getenv("WUNDERGROUND_ENDPOINT")
WUNDERGROUND_DAILY_ENDPOINT = os.getenv("WUNDERGROUND_DAILY_ENDPOINT")
HOLFUY_LIVE_ENDPOINT = os.getenv("HOLFUY_LIVE_ENDPOINT")
HOLFUY_HISTORIC_ENDPOINT = os.getenv("HOLFUY_HISTORIC_ENDPOINT")
THINGSPEAK_ENDPOINT = os.getenv("THINGSPEAK_ENDPOINT")
ECOWITT_ENDPOINT = os.getenv("ECOWITT_ENDPOINT")
ECOWITT_DAILY_ENDPOINT = os.getenv("ECOWITT_DAILY_ENDPOINT")
# endregion

#region argument processing
def get_args():
    parser = argparse.ArgumentParser(description="Weather Station Data Processor")
    parser.add_argument("--all", action="store_true", help="Read all stations")
    parser.add_argument("--type", type=str, help="Read stations with a specific type")
    parser.add_argument("--id", type=str, help="Read a single weather station by id")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run")
    parser.add_argument("--single-thread", action="store_true", help="Run in single-thread mode", default=False)
    return parser.parse_args()

def validate_args(args):
    if args.all and args.type:
        raise ValueError("Cannot specify both --all and --type")
    if args.all and args.id:
        raise ValueError("Cannot specify both --all and --id")
    if args.type and args.id:
        raise ValueError("Cannot specify both --type and --id")
    
    if not args.all and not args.type and not args.id:
        raise ValueError("Must specify --all, --type or --id")
#endregion

# region processing

class Gatherer:

    def __init__(self, run_id: str, dry_run: bool):
        self.run_id = run_id
        self.dry_run = dry_run
        self.stations = set()
        self.readers = {
            'meteoclimatic':    api.MeteoclimaticReader(),
            'weatherlink_v1':   api.WeatherLinkV1Reader(live_endpoint=WEATHERLINK_V1_ENDPOINT),
            'wunderground':     api.WundergroundReader(live_endpoint=WUNDERGROUND_ENDPOINT, daily_endpoint=WUNDERGROUND_DAILY_ENDPOINT),
            'weatherlink_v2':   api.WeatherlinkV2Reader(live_endpoint=WEATHERLINK_V2_ENDPOINT, daily_endpoint=WEATHERLINK_V2_ENDPOINT),
            'holfuy':           api.HolfuyReader(live_endpoint=HOLFUY_LIVE_ENDPOINT, daily_endpoint=HOLFUY_HISTORIC_ENDPOINT),
            'thingspeak':       api.ThingspeakReader(live_endpoint=THINGSPEAK_ENDPOINT),
            'ecowitt':          api.EcowittReader(live_endpoint=ECOWITT_ENDPOINT, daily_endpoint=ECOWITT_DAILY_ENDPOINT),
            'realtime':         api.RealtimeReader()
        }

    def add_station(self, station):
        if station is None:
            logging.error("Station is None")
            return
        
        if station in self.stations:
            logging.error(f"Station {station} already saved")
            return
        
        self.stations.add(station)

    def add_many(self, stations):
        if stations is None:
            logging.error("Stations is None")
            return
        
        for station in stations:
            self.add_station(station)

    def process_station(self, station: WeatherStation):
        logging.info(f"Processing station {station.id}, type {station.connection_type}")

        # Validate timezone
        valid_timezones = [
            'Europe/Madrid',
            'Europe/Lisbon',
            'Etc/UTC'
        ]

        if data_timezone not in valid_timezones:
            logging.error(f"Invalid data timezone for station {station.id}. Defaulting to UTC.")
            data_timezone = 'Etc/UTC'

        if local_timezone not in valid_timezones:
            logging.error(f"Invalid local timezone for station {station.id}. Defaulting to UTC.")
            local_timezone = 'Etc/UTC'

        data_timezone = ZoneInfo(data_timezone)
        local_timezone = ZoneInfo(local_timezone)
        
        try:
            # Get the handler function based on connection_type or use a default handler
            reader = self.readers.get(station.connection_type)
            
            if station.connection_type == "connection_disabled":
                    return {"status": "success"}

            if reader is None:
                message = f"Invalid connection type for station {station.id}"
                logging.error(f"{message}")
                return {"status": "error", "error": message}
                
            # Call the appropriate handler
            record = reader.get_data(station)
            
            if record is None:
                message = f"No data retrieved for station {station.id}"
                logging.error(f"{message}")
                return {"status": "error", "error": message}

            record.station_id = station.id
            record.gatherer_thread_id = self.run_id

            record.sanity_check()
            record.apply_pressure_offset(station.pressure_offset)
            record.apply_rounding(1)

            if not self.dry_run:
                Database.save_record(record)
                logging.info(f"Record saved for station {station.id}")
                return {"status": "success"}
            else:
                logging.debug(json.dumps(record.__dict__, indent=4, sort_keys=True, default=str))
                logging.info(f"Dry run enabled, record not saved for station {station.id}")
                return {"status": "success"}

        except Exception as e:
            logging.error(f"Error processing station {station.id}: {e}")
            if not self.dry_run:
                increment_incident_count(station.id)
            return {"status": "error", "error": str(e)}
    
    def process_chunk(self, chunk, chunk_number, result_queue):
        logging.info(f"Processing chunk {chunk_number} on {threading.current_thread().name}")

        results = {}
        for station in chunk:
            results[station.id] = self.process_station(station)
        
        result_queue.put(results)

    def multithread_processing(self, stations):
        chunk_size = len(stations) // MAX_THREADS
        remainder_size = len(stations) % MAX_THREADS
        chunks = []
        for i in range(MAX_THREADS):
            start = i * chunk_size
            end = start + chunk_size
            chunks.append(stations[start:end])
        for i in range(remainder_size):
            chunks[i].append(stations[-(i+1)])
            
        result_queue = queue.Queue()
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = [executor.submit(self.process_chunk, chunk, i, result_queue) for i, chunk in enumerate(chunks)]
            concurrent.futures.wait(futures)
        
        results = {}
        while not result_queue.empty():
            results.update(result_queue.get())
        
        return results

    def process(self, single_thread: bool):
        if len(self.stations) == 0:
            logging.error("No active stations found!")
            return
        single_thread = single_thread or len(self.stations) < 30

        results = {}

        if single_thread:
            for station in self.stations:
                results[station.id] = self.process_station(station)

        else:
            results = self.multithread_processing(list(self.stations))

        return results


# endregion

# region main
def main():
    setup_logger()

    logging.info("Starting gatherer service.")
    
    logging.info(f"Connecting to database...")
    Database.initialize(DB_URL)
    logging.info("Connected to database.")

    args = get_args()
    validate_args(args)

    dry_run = args.dry_run
    single_thread = args.single_thread

    run_id = uuid4().hex
    timestamp = datetime.now().replace(second=0, microsecond=0)

    gatherer = Gatherer(run_id, dry_run)
    
    if args.dry_run:
        logging.warning("[Dry run enabled]")
        set_debug_mode()
    else:
        logging.warning("[Dry run disabled]")
        Database.init_thread_record(run_id, timestamp, command=" ".join(os.sys.argv))

    logging.info(f"Starting gatherer run {run_id}")

    if args.id:
        gatherer.add_station(get_single_station(args.id))
    elif args.type:
        gatherer.add_many(get_stations_by_connection_type(args.type))
    else:
        gatherer.add_many(get_all_stations())
    
    results = gatherer.process(single_thread)
                
    if not args.dry_run:
        logging.info("Saving thread record")
        Database.save_thread_record(run_id, results)

    Database.close_all_connections()

if __name__ == "__main__":
    main()

# endregion