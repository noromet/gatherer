import os
from dotenv import load_dotenv
import concurrent.futures
import threading
import json
import argparse

from database import Database, get_all_stations, get_single_station, get_stations_by_type, increment_incident_count
import weather_readers as api

# region definitions
print_red = lambda text: print(f"\033[91m{text}\033[00m")
print_green = lambda text: print(f"\033[92m{text}\033[00m")
print_yellow = lambda text: print(f"\033[93m{text}\033[00m")
    
load_dotenv(verbose=True)
DB_URL = os.getenv("DATABASE_CONNECTION_URL")
MAX_THREADS = int(os.getenv("MAX_THREADS"))
WEATHERLINK_V1_ENDPOINT = os.getenv("WEATHERLINK_V1_ENDPOINT")
WEATHER_DOT_COM_ENDPOINT = os.getenv("WEATHER_DOT_COM_ENDPOINT")
DRY_RUN = False
# endregion

#region argument processing
def get_args():
    parser = argparse.ArgumentParser(description="Weather Station Data Processor")
    parser.add_argument("--all", action="store_true", help="Read all stations")
    parser.add_argument("--type", type=str, help="Read stations with a specific type")
    parser.add_argument("--id", type=str, help="Read a single weather station by id")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run")
    parser.add_argument("--multithread-threshold", type=int, default=-1, help="Threshold for enabling multithreading")
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
        if args.type not in ["meteoclimatic", "weatherlink_v1", "weatherdotcom"]:
            raise ValueError("Invalid type")
        
    if args.multithread_threshold == 0 or args.multithread_threshold < -1: #so, -1 or positive integer are valid
        raise ValueError("Invalid multithread threshold")
#endregion

# region processing
def process_station(station: tuple): # station is a tuple like id, connection_type, field1, field2, field3
    print_yellow(f"Processing station {station[0]}, type {station[1]}")
    
    try:
        if station[1] == 'meteoclimatic':
            record = api.MeteoclimaticReader.get_data(station[2])
        elif station[1] == 'weatherlink_v1':
            record = api.WeatherLinkV1Reader.get_data(WEATHERLINK_V1_ENDPOINT, station[2:])
        elif station[1] == 'weatherdotcom':
            record = api.WeatherDotComReader.get_data(WEATHER_DOT_COM_ENDPOINT, station[2:])
        else:
            print(f"Unknown station type {station[1]} for station {station[0]}")
            return
        
        if record is None:
            print(f"No data retrieved for station {station[0]}")
            return

        record.station_id = station[0]

        if not DRY_RUN:
            Database.save_record(record)
            print_green(f"Record saved for station {station[0]}")
        else:
            print(json.dumps(record.__dict__, indent=4, sort_keys=True, default=str))
            print_green(f"Dry run enabled, record not saved for station {station[0]}")

    except Exception as e:
        print_red(f"Error processing station {station[0]}: {e}")
        if not DRY_RUN:
            increment_incident_count(station[0])
    print()

def process_chunk(chunk, chunk_number):
    print(f"Processing chunk {chunk_number} on {threading.current_thread().name}")
    for station in chunk:
        process_station(station)

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
        
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for i, chunk in enumerate(chunks):
            executor.submit(process_chunk, chunk, chunk_number=i)

def process_all(multithread_threshold):
    stations = get_all_stations()

    if len(stations) == 0:
        print_red("No active stations found!")
        return

    print(f"Processing {len(stations)} stations")
    
    if multithread_threshold == -1 or len(stations) < multithread_threshold:
        for station in stations:
            process_station(station)

    elif len(stations) >= multithread_threshold:
        multithread_processing(stations)

    else:
        raise ValueError("Invalid multithread threshold")
    
def process_single(station_id):
    station = get_single_station(station_id)
    if station is None:
        print_red(f"Station {station_id} not found")
        return
    process_station(station)

def process_type(station_type, multithread_threshold):
    stations = get_stations_by_type(station_type)
    if len(stations) == 0:
        print_red(f"No active stations found for type {station_type}")
        return
    
    print(f"Processing {len(stations)} stations of type {station_type}")

    if multithread_threshold == -1 or len(stations) < multithread_threshold:
        for station in stations:
            process_station(station)

    elif len(stations) >= multithread_threshold:
        multithread_processing(stations)

    else:
        raise ValueError("Invalid multithread threshold")

# endregion

# region main
def main():
    Database.initialize(DB_URL)

    args = get_args()
    validate_args(args)

    global DRY_RUN
    DRY_RUN = args.dry_run
    multithread_threshold = args.multithread_threshold

    if args.dry_run:
        print_yellow("[Dry run enabled]")
    else:
        print_yellow("[Dry run disabled]")

    if args.id:
        process_single(args.id)
    
    elif args.type:
        process_type(args.type, multithread_threshold)

    else:
        process_all(multithread_threshold)
                
    Database.close_all_connections()
    
if __name__ == "__main__":
    main()

# endregion