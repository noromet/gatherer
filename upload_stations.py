"""
Reads a CSV file containing station data and uploads it to the database.
"""

import psycopg2
import csv
import os
import uuid
from dotenv import load_dotenv
from datetime import datetime
import json

def print_red(text):
    print(f"\033[91m{text}\033[00m")
    
def print_green(text):
    print(f"\033[92m{text}\033[00m")

def main():
    load_dotenv(verbose=True)
    db_url = os.getenv("DATABASE_CONNECTION_URL")
    owner_id = os.getenv("STATION_OWNER_ID")
    
    print_red("This script will upload the weather stations from the file 'estaciones.csv' to the database.")
    print(f"Environment variables: \n\tDATABASE_CONNECTION_URL={db_url}\n\tSTATION_OWNER_ID={owner_id}")
    input("Press Enter to confirm.")   
    
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    
    # read file from csv estaciones.csv
    with open('estaciones.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';', quotechar='"')
        
        for row in reader:
            weather_station = {
                "id": str(uuid.uuid4()),
                "owner_id": owner_id,
                "name": row["name"],
                "status": "blocked",
                "location": row["location"],
                "province": row["province"],
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
                "elevation": float(row["elevation"]),
                "did": row["did"],
                "token": row["token"],
                "model": row["model"],
                "brand": row["brand"],
                "date": datetime.strptime(row["date"], "%Y-%m-%d")
            }
            
            print_green(f"Inserting station {weather_station["id"]}, location {weather_station["location"]}...")
            
            cursor.execute("""
                INSERT INTO weather_station (id, owner_id, name, status, location, province, latitude, longitude, elevation, created_at, token, model, brand)
                VALUES (%(id)s, %(owner_id)s, %(name)s, %(status)s, %(location)s, %(province)s, %(latitude)s, %(longitude)s, %(elevation)s, %(date)s, %(token)s, %(model)s, %(brand)s)
            """, weather_station)
            conn.commit()
            
            print_green(f"Station {weather_station["id"]} inserted successfully.")
            print()
        
if __name__ == "__main__":
    main()