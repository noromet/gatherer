import os
from dotenv import load_dotenv
import json
import concurrent.futures
import threading

from database import Database, get_all_stations
from api import MeteoclimaticReader

def process_chunk(chunk, chunk_number):
    print(f"Processing chunk {chunk_number} on {threading.current_thread().name}")
    for station in chunk:
        print(f"\tProcessing station {station['id']}, type {station['type']} on {chunk_number}")
        
        #get data from station
        params = station['connection_params']
        if station['type'] == 'meteoclimatic':
            try:
                record = MeteoclimaticReader.get_data(params['endpoint'])
                record.station_id = station['id']
                Database.save_record(record)
            except Exception as e:
                print(f"Error processing station {station['id']}: {e}")
        else:
            pass

def main():
    load_dotenv(verbose=True)
    db_url = os.getenv("DATABASE_CONNECTION_URL")
    max_threads = int(os.getenv("MAX_THREADS"))
    
    Database.initialize(db_url)
    stations = get_all_stations()
    print(json.dumps(stations, indent=2))
    
    #divide the station list in chunks to be processed by threads
    chunk_size = len(stations) // max_threads
    remainder_size = len(stations) % max_threads
    chunks = []
    for i in range(max_threads):
        start = i * chunk_size
        end = start + chunk_size
        chunks.append(stations[start:end])
    for i in range(remainder_size):
        chunks[i].append(stations[-(i+1)])
        
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        for i, chunk in enumerate(chunks):
            executor.submit(process_chunk, chunk, chunk_number=i)
            
    Database.close_all_connections()
    
if __name__ == "__main__":
    main()

