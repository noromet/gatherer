"""
Reads a CSV file containing station data and uploads it to the database.
"""

import psycopg2
import csv
import os
import uuid
from dotenv import load_dotenv
from datetime import datetime

tipo_to_connection_type_map = {
    # '0': "weatherlink_v1",
    # '1': "meteoclimatic",
    # '3': "wunderground",
    # '5': "weatherlink_v2",
    '6': "holfuy",
}

def print_red(text):
    print(f"\033[91m{text}\033[00m")
    
def print_green(text):
    print(f"\033[92m{text}\033[00m")

def na_to_none(value):
    if not isinstance(value, str):
        return value
    return None if value.strip().lower() == "na" else value

def main():
    load_dotenv(verbose=True)
    db_url = os.getenv("DATABASE_CONNECTION_URL")
    owner_email = os.getenv("STATION_OWNER_EMAIL")
    
    print_red("This script will upload the weather stations from the file 'estaciones.csv' to the database.")
    print(f"Environment variables: \n\tDATABASE_CONNECTION_URL={db_url}\n\tSTATION_OWNER_EMAIL={owner_email}")
    
    input("Press Enter to confirm.")   
    
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    
    # get user id
    cursor.execute("SELECT \"id\" FROM \"user\" WHERE \"email\" = %s", (owner_email,))
    owner_id = cursor.fetchone()
    
    # read file from csv estaciones.csv
    counter_added = 0
    with open('estaciones.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';', quotechar='"')
        
        for row in reader:
            old_conn_type_int = row["tipo"]
            if old_conn_type_int not in tipo_to_connection_type_map:
                print_red(f"Unsupported connection type '{old_conn_type_int}'. Skipping...")
                continue
            connection_type = tipo_to_connection_type_map[old_conn_type_int]
            
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
                "model": row["model"],
                "brand": row["brand"],
                "date": datetime.strptime(row["date"], "%Y-%m-%d"),
                "connection_type": connection_type,
                "field1": na_to_none(row["did"]),
                "field2": na_to_none(row["token"]),
                "field3": na_to_none(row["password"])
            }
            
            print_green(f'Inserting station {weather_station["id"]}, location {weather_station["location"]}...')
            
            cursor.execute("""
                INSERT INTO weather_station (id, owner_id, name, status, location, province, latitude, longitude, elevation, model, brand, created_at, connection_type, field1, field2, field3)
                VALUES (%(id)s, %(owner_id)s, %(name)s, %(status)s, %(location)s, %(province)s, %(latitude)s, %(longitude)s, %(elevation)s, %(model)s, %(brand)s, %(date)s, %(connection_type)s, %(field1)s, %(field2)s, %(field3)s)
            """, weather_station)
            conn.commit()
            
            print_green(f'Station {weather_station["id"]} inserted successfully.')
            counter_added += 1

    print_green(f"\n***\nAdded {counter_added} stations.\n***")
        
if __name__ == "__main__":
    main()