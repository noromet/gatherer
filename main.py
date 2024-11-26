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
DRY_RUN = False

RUN_ID = uuid4().hex
# endregion

#force these commands:
# --all --save-thread-record --multithread-threshold 2
# import sys
# sys.argv = ["main.py", "--all", "--save-thread-record", "--multithread-threshold", "2"]

#region argument processing
def get_args():
    parser = argparse.ArgumentParser(description="Weather Station Data Processor")
    parser.add_argument("--all", action="store_true", help="Read all stations")
    parser.add_argument("--type", type=str, help="Read stations with a specific type")
    parser.add_argument("--id", type=str, help="Read a single weather station by id")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run")
    parser.add_argument("-m", "--multithread-threshold", type=int, default=-1, help="Threshold for enabling multithreading")
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
        if args.type not in ["meteoclimatic", "weatherlink_v1", "wunderground", "weatherlink_v2", "holfuy", "thingspeak"]:
            raise ValueError("Invalid type")
        
    if args.multithread_threshold == 0 or args.multithread_threshold < -1: #so, -1 or positive integer are valid
        raise ValueError("Invalid multithread threshold")
#endregion

# region processing
def process_station(station: tuple): # station is a tuple like id, connection_type, field1, field2, field3
    print_yellow(f"Processing station {station[0]}, type {station[1]}")
    
    try:
        if station[1] == 'connection_disabled':
            message = f"Connection disabled for station {station[0]}"
            print(message)
            return {"status": "success"}

        if station[1] == 'meteoclimatic':
            record = api.MeteoclimaticReader.get_data(station[2])
        elif station[1] == 'weatherlink_v1':
            record = api.WeatherLinkV1Reader.get_data(WEATHERLINK_V1_ENDPOINT, station[2:])
        elif station[1] == 'wunderground':
            record = api.WundergroundReader.get_data(WUNDERGROUND_ENDPOINT, WUNDERGROUND_DAILY_ENDPOINT, station[2:])
        elif station[1] == 'weatherlink_v2':
            record = api.WeatherlinkV2Reader.get_data(WEATHERLINK_V2_ENDPOINT, station[2:])
        elif station[1] == 'holfuy':
            record = api.HolfuyReader.get_data(HOLFUY_ENDPOINT, station[2:])
        elif station[1] == 'thingspeak':
            record = api.ThingspeakReader.get_data(THINGSPEAK_ENDPOINT, station[2:])
        else:
            message = f"Unknown station type {station[1]} for station {station[0]}"
            print(message)
            return {"status": "error", "error": message}
        
        if record is None:
            message = f"No data retrieved for station {station[0]}"
            print(message)
            return {"status": "error", "error": message}

        record.station_id = station[0]
        record.gatherer_run_id = RUN_ID

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

def process_all(multithread_threshold):
    stations = get_all_stations()

    result = {}

    if len(stations) == 0:
        print_red("No active stations found!")
        return

    print(f"Processing {len(stations)} stations")
    
    if multithread_threshold == -1 or len(stations) < multithread_threshold:
        for station in stations:
            result[station[0]] = process_station(station)

    elif len(stations) >= multithread_threshold:
        result = multithread_processing(stations)

    else:
        raise ValueError("Invalid multithread threshold")
    
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

def process_type(station_type, multithread_threshold):
    stations = get_stations_by_type(station_type)
    if len(stations) == 0:
        print_red(f"No active stations found for type {station_type}")
        return

    result = {}
    
    print(f"Processing {len(stations)} stations of type {station_type}")

    if multithread_threshold == -1 or len(stations) < multithread_threshold:
        for station in stations:
            result[station[0]] = process_station(station)

    elif len(stations) >= multithread_threshold:
        result = multithread_processing(stations)

    else:
        raise ValueError("Invalid multithread threshold")

    return result

# endregion

# region main
def main():
    Database.initialize(DB_URL)

    args = get_args()
    validate_args(args)

    global DRY_RUN
    DRY_RUN = args.dry_run
    multithread_threshold = args.multithread_threshold

    timestamp = datetime.now().replace(second=0, microsecond=0)
    
    if args.dry_run:
        print_yellow("[Dry run enabled]")
    else:
        print_yellow("[Dry run disabled]")
        Database.init_thread_record(RUN_ID, timestamp, command=" ".join(os.sys.argv))

    if args.id:
        results = process_single(args.id)
    elif args.type:
        results = process_type(args.type, multithread_threshold)
    else:
        results = process_all(multithread_threshold)
                
    if not args.dry_run:
        print_yellow("Saving thread record")
        Database.save_thread_record(RUN_ID, results)

    Database.close_all_connections()

if __name__ == "__main__":
    main()

# endregion