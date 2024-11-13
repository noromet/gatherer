import csv
import psycopg2

# Read the CSV file and extract the organization names
csv_file = 'estaciones.csv'
organization_names = set()

with open(csv_file, mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file, delimiter=';')
    for row in reader:
        organization_names.add(row['name'])

print(organization_names)

# Connect to the PostgreSQL database
conn = psycopg2.connect("postgresql://rhean:password@127.0.0.1:5432/noromet")
cursor = conn.cursor()

# Fetch organization IDs
organization_ids = {}
for name in organization_names:
    cursor.execute("SELECT id FROM organization WHERE name = %s", (name,))
    result = cursor.fetchone()
    if result:
        organization_ids[name] = result[0]
    else:
        print(f"Organization not found: {name}")

print(organization_ids)

# Generate SQL update queries
update_queries = []

with open(csv_file, mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file, delimiter=';')
    for row in reader:
        org_name = row['name']
        location = row['location'] #to query db and get the station id
        org_id = organization_ids.get(org_name)
        
        station_id = None
        cursor.execute("SELECT id FROM weather_station WHERE location = %s", (location,))
        result = cursor.fetchone()

        if result:
            station_id = result[0]
        else:
            print(f"Station not found for location {location}")
            continue

        update_query = f"UPDATE weather_station SET organization_id = '{org_id}' WHERE id = '{station_id}'"
        update_queries.append(update_query)

# Execute the update queries
for query in update_queries:
    cursor.execute(query)

# Commit the changes and close the connection
conn.commit()
conn.close()