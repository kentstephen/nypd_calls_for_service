import concurrent.futures
import requests
import time
import psycopg2.pool
from A_postgres_config import POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER
import math 

# api url
url = "https://data.cityofnewyork.us/api/odata/v4/d6zx-ckhd"
#number of records per request, the max for this api
records_per_request = 50000
#number of workers
max_workers = 20

# select statement using SQL to figure out how many rows is in the dataset
len_url = 'https://data.cityofnewyork.us/resource/d6zx-ckhd.json?$select=count(*)'
# using requests to store the response
response = requests.get(len_url)
#getting the json outpu
data = response.json()
print(data)
# the response is a dictionary with one entry so we index to 0 the first one then call for the key value
num_records = data[0]['count']
#print(num_records)
# we need to format the number of requests so it's divisble by 50,000 then below we round up
moved_decimal = int(num_records) / int(records_per_request)
num_requests = math.ceil(moved_decimal)

# Create a connection pool
pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=max_workers,
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)

def connect_to_db():
    """Establishes a connection to the PostgreSQL database."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    return conn

def fetch_and_insert_data(skip):
    """Fetches data from the API and inserts it into the database."""
    # Get a connection from the pool
    with pool.getconn() as conn:
        # Create a session object
        session = requests.Session()

        # Retry mechanism for failed requests
        for i in range(5):
            try:
                # Build the URL for the current request
                request_url = f"{url}?$top={records_per_request}&$skip={skip}"
                print(request_url)
                # Make the request using the session object
                response = session.get(request_url)

                # Parse the JSON response
                data = response.json()

                # Prepare data for insertion
                records_to_insert = [
                (
                    row['__id'], row['objectid'], row['cad_evnt_id'],
                    row['create_date'], row['incident_date'], row['incident_time'],
                    row['nypd_pct_cd'], row['boro_nm'], row['patrl_boro_nm'],
                    row['geo_cd_x'], row['geo_cd_y'], row['radio_code'],
                    row['typ_desc'], row['cip_jobs'], row['add_ts'],
                    row['disp_ts'], row['arrivd_ts'], row['closng_ts'],
                    row['latitude'], row['longitude']
                )
                for row in data['value']
            ]

            #print(f"Fetched {len(records_to_insert)} records starting from {skip}")

            # Insert data into the database
            #print("Inserting records into the database")
                with conn.cursor() as curs:
                    curs.executemany(
                        "INSERT INTO sch_nypd_calls_tables.tb_call_data_conc_2 (__id, objectid, cad_evnt_id, create_date, incident_date, incident_time, nypd_pct_cd, boro_nm, patrl_boro_nm, geo_cd_x, geo_cd_y, radio_code, typ_desc, cip_jobs, add_ts, disp_ts, arrivd_ts, closng_ts, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        records_to_insert
                    )
                    conn.commit()

                # Exit the retry loop if the request was successful
                break
            except Exception as e:
                print(f"Error occurred while processing records from {skip}: {e}. Attempt #{i + 1}. Retrying...")
                # Sleep before retrying
                time.sleep(2)

        # Close the session object
        session.close()

if __name__ == '__main__':
    # Start the ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Calculate the skip values for each worker
        skips = [i * records_per_request for i in range(num_requests)]

        # Start the workers
        executor.map(fetch_and_insert_data, skips)

    print("All data fetched and inserted.")

# Close the connection pool
pool.closeall()