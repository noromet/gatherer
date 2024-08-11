import os
from dotenv import load_dotenv
import json
import concurrent.futures
import threading

from database import Database, get_all_stations
import api

load_dotenv(verbose=True)
DB_URL = os.getenv("DATABASE_CONNECTION_URL")
MAX_THREADS = int(os.getenv("MAX_THREADS"))
WEATHERLINK_ENDPOINT = os.getenv("WEATHERLINK_ENDPOINT")
WEATHER_DOT_COM_ENDPOINT = os.getenv("WEATHER_DOT_COM_ENDPOINT")

def process_chunk(chunk, chunk_number):
    print(f"Processing chunk {chunk_number} on {threading.current_thread().name}")
    for station in chunk:
        print(f"\tProcessing station {station['id']}, type {station['type']} on {chunk_number}")
        
        #get data from station
        params = station['connection_params']
        
        if station['type'] == 'meteoclimatic':
            try:
                record = api.MeteoclimaticReader.get_data(params['endpoint'])
                record.station_id = station['id']
                Database.save_record(record)
                print(f"Record saved for station {station['id']}")
            except Exception as e:
                print(f"Error processing station {station['id']}: {e}")
                
        elif station['type'] == 'weatherlink':
            try:
                record = api.WeatherLinkReader.get_data(WEATHERLINK_ENDPOINT, params)
                record.station_id = station['id']
                Database.save_record(record)
                print(f"Record saved for station {station['id']}")
            except Exception as e:
                print(f"Error processing station {station['id']}: {e}")
                
        elif station['type'] == 'weatherdotcom':
            try:
                record = api.WeatherDotComReader.get_data(WEATHER_DOT_COM_ENDPOINT, params)
                record.station_id = station['id']
                Database.save_record(record)
                print(f"Record saved for station {station['id']}")
            except Exception as e:
                print(f"Error processing station {station['id']}: {e}")
        else:
            pass

def main():
    
    Database.initialize(DB_URL)
    stations = get_all_stations()
    print(json.dumps(stations, indent=2))
    
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
            
    Database.close_all_connections()
    
if __name__ == "__main__":
    main()

