import os
from dotenv import load_dotenv
import concurrent.futures
import threading
import sys

from database import Database, get_all_stations, get_single_station
import api

def print_red(text):
    print(f"\033[91m{text}\033[00m")
    
def print_green(text):
    print(f"\033[92m{text}\033[00m")

def print_yellow(text):
    print(f"\033[93m{text}\033[00m")
    
load_dotenv(verbose=True)
DB_URL = os.getenv("DATABASE_CONNECTION_URL")
MAX_THREADS = int(os.getenv("MAX_THREADS"))
WEATHERLINK_V1_ENDPOINT = os.getenv("WEATHERLINK_V1_ENDPOINT")
WEATHER_DOT_COM_ENDPOINT = os.getenv("WEATHER_DOT_COM_ENDPOINT")

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
        Database.save_record(record)
        print_green(f"Record saved for station {station[0]}")
    except Exception as e:
        print_red(f"Error processing station {station[0]}: {e}")
    print()

def process_chunk(chunk, chunk_number):
    print(f"Processing chunk {chunk_number} on {threading.current_thread().name}")
    for station in chunk:
        process_station(station)

def main():
    Database.initialize(DB_URL)
    
    if len(sys.argv) > 1:
        assert len(sys.argv[1]) > 1
        station_id = sys.argv[1]
        station = get_single_station(station_id)
        process_station(station)
    
    else:
        stations = get_all_stations()
        
        if len(stations) > 50:
            #divide the station list in chunks to be processed by threads
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
        else:
            # Process stations without multithreading
            for station in stations:
                process_station(station)
                
        Database.close_all_connections()
    
if __name__ == "__main__":
    main()
