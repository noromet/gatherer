import os
from dotenv import load_dotenv
import concurrent.futures
import threading
import json
import argparse
import queue
from database import Database, get_all_stations, get_single_station, get_stations_by_type, increment_incident_count
import weather_readers as api
from datetime import datetime
from uuid import uuid4
import logging
from logging.handlers import RotatingFileHandler
# region definitions
print_red = lambda text: print(f"\033[91m{text}\033[00m")
print_green = lambda text: print(f"\033[92m{text}\033[00m")
print_yellow = lambda text: print(f"\033[93m{text}\033[00m")
    
load_dotenv(verbose=True)
DB_URL = os.getenv("DATABASE_CONNECTION_URL")
MAX_THREADS = int(os.getenv("MAX_THREADS"))
WEATHERLINK_V1_ENDPOINT = os.getenv("WEATHERLINK_V1_ENDPOINT")
WEATHERLINK_V2_ENDPOINT = os.getenv("WEATHERLINK_V2_ENDPOINT")
WUNDERGROUND_ENDPOINT = os.getenv("WUNDERGROUND_ENDPOINT")
WUNDERGROUND_DAILY_ENDPOINT = os.getenv("WUNDERGROUND_DAILY_ENDPOINT")
HOLFUY_ENDPOINT = os.getenv("HOLFUY_ENDPOINT")
THINGSPEAK_ENDPOINT = os.getenv("THINGSPEAK_ENDPOINT")
ECOWITT_ENDPOINT = os.getenv("ECOWITT_ENDPOINT")
ECOWITT_DAILY_ENDPOINT = os.getenv("ECOWITT_DAILY_ENDPOINT")
DRY_RUN = False

RUN_ID = uuid4().hex
# endregion

# region logging
# Set up a specific logger with our desired output level
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Add the log message handler to the logger
handler = RotatingFileHandler(
    "gatherer.log", maxBytes=5*1024*1024, backupCount=5
)
handler.setLevel(logging.INFO)

# Create a formatter and set the formatter for the handler
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)
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
    
    if args.type:
        if args.type not in ["meteoclimatic", "weatherlink_v1", "wunderground", "weatherlink_v2", "holfuy", "thingspeak", "ecowitt"]:
            raise ValueError("Invalid type")
#endregion

# region processing
def process_station(station: tuple): # station is a tuple like id, connection_type, field1, field2, field3, pressure_offset
    print_yellow(f"Processing station {station[0]}, type {station[1]}")
    
    try:
        if station[1] == 'connection_disabled':
            message = f"Connection disabled for station {station[0]}"
            print(message)
            return {"status": "success"}

        if station[1] == 'meteoclimatic':
            record = api.MeteoclimaticReader.get_data(station[2], station_id=station[0])
        elif station[1] == 'weatherlink_v1':
            record = api.WeatherLinkV1Reader.get_data(WEATHERLINK_V1_ENDPOINT, station[2:5], station_id=station[0])
        elif station[1] == 'wunderground':
            record = api.WundergroundReader.get_data(WUNDERGROUND_ENDPOINT, WUNDERGROUND_DAILY_ENDPOINT, station[2:5], station_id=station[0])
        elif station[1] == 'weatherlink_v2':
            raise NotImplementedError("Weatherlink V2 is not implemented yet")
            # record = api.WeatherlinkV2Reader.get_data(WEATHERLINK_V2_ENDPOINT, station[2:5], station_id=station[0])
        elif station[1] == 'holfuy':
            record = api.HolfuyReader.get_data(HOLFUY_ENDPOINT, station[2:5], station_id=station[0])
        elif station[1] == 'thingspeak':
            record = api.ThingspeakReader.get_data(THINGSPEAK_ENDPOINT, station[2:5], station_id=station[0])
        else:
            record = api.EcowittReader.get_data(ECOWITT_ENDPOINT, ECOWITT_DAILY_ENDPOINT, station[2:5], station_id=station[0])
        
        if record is None:
            message = f"No data retrieved for station {station[0]}"
            print(f"[{station[0]}]: {message}")
            logging.error(f"[{station[0]}]: {message}")
            return {"status": "error", "error": message}

        record.station_id = station[0]
        record.gatherer_run_id = RUN_ID

        record.sanity_check()
        record.apply_pressure_offset(station[5])

        if not DRY_RUN:
            Database.save_record(record)
            print_green(f"Record saved for station {station[0]}")
            return {"status": "success"}
        else:
            print(json.dumps(record.__dict__, indent=4, sort_keys=True, default=str))
            print_green(f"Dry run enabled, record not saved for station {station[0]}")
            return {"status": "success"}

    except Exception as e:
        print_red(f"Error processing station {station[0]}: {e}")
        logging.error(f"Error processing station {station[0]}: {e}")
        if not DRY_RUN:
            increment_incident_count(station[0])
        return {"status": "error", "error": str(e)}
    
def process_chunk(chunk, chunk_number, result_queue):
    print(f"Processing chunk {chunk_number} on {threading.current_thread().name}")

    results = {}
    for station in chunk:
        results[station[0]] = process_station(station)
    
    result_queue.put(results)

def multithread_processing(stations):
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
        futures = [executor.submit(process_chunk, chunk, i, result_queue) for i, chunk in enumerate(chunks)]
        concurrent.futures.wait(futures)
    
    results = {}
    while not result_queue.empty():
        results.update(result_queue.get())
    
    return results

def process_all(single_thread):
    stations = get_all_stations()

    result = {}

    if len(stations) == 0:
        print_red("No active stations found!")
        return

    print(f"Processing {len(stations)} stations")
    logging.info(f"Processing {len(stations)} stations")
    
    if single_thread:
        for station in stations:
            result[station[0]] = process_station(station)
    else:
        result = multithread_processing(stations)
    
    return result

def process_single(station_id):
    station = get_single_station(station_id)
    if station is None:
        print_red(f"Station {station_id} not found")
        return
    process_station(station)

    return {station_id: {
        "status": "success"
    }}

def process_type(station_type, single_thread):
    stations = get_stations_by_type(station_type)
    if len(stations) == 0:
        print_red(f"No active stations found for type {station_type}")
        return

    result = {}
    
    print(f"Processing {len(stations)} stations of type {station_type}")
    logging.info(f"Processing {len(stations)} stations of type {station_type}")

    if single_thread:
        for station in stations:
            result[station[0]] = process_station(station)
    else:
        result = multithread_processing(stations)

    return result

# endregion

# region main
def main():
    Database.initialize(DB_URL)

    args = get_args()
    validate_args(args)

    global DRY_RUN
    DRY_RUN = args.dry_run
    single_thread = args.single_thread

    timestamp = datetime.now().replace(second=0, microsecond=0)
    
    if args.dry_run:
        print_yellow("[Dry run enabled]")
    else:
        print_yellow("[Dry run disabled]")
        Database.init_thread_record(RUN_ID, timestamp, command=" ".join(os.sys.argv))

    logging.info(f"Starting gatherer run {RUN_ID}")

    if args.id:
        results = process_single(args.id)
    elif args.type:
        results = process_type(args.type, single_thread)
    else:
        results = process_all(single_thread)
                
    if not args.dry_run:
        print_yellow("Saving thread record")
        Database.save_thread_record(RUN_ID, results)

    Database.close_all_connections()

if __name__ == "__main__":
    main()

# endregion