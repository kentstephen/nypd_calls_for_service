from postgres_config import POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER
import concurrent.futures
import requests
import psycopg2.pool
from psycopg2.extras import execute_batch
import math
import time
from datetime import datetime

# Constants and Configuration
API_URL = "https://data.cityofnewyork.us/resource/d6zx-ckhd.json"
RECORDS_PER_REQUEST = 50000  # Number of records per request
SLEEP_INTERVAL = 60  # Seconds to sleep after processing a significant number of records

# Global Session Object for Efficient HTTP Requests
session = requests.Session()

# Database Connection Pool Initialization
pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1, maxconn=15,
    host=POSTGRES_HOST, port=POSTGRES_PORT,
    dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD
)


# Function to Fetch Data from API and Insert into Database
def fetch_and_insert_data(skip):
    conn = pool.getconn()
    try:
        conn.autocommit = False
        with conn.cursor() as curs:
            attempt = 0
            max_attempts = 5
            success = False

            while attempt < max_attempts and not success:
                try:
                    response = session.get(f"{API_URL}?$limit={RECORDS_PER_REQUEST}&$offset={skip}")
                    if response.status_code == 200:
                        data = response.json()
                        if not data:
                            print(f"No data received for skip={skip}. Exiting.")
                            return

                        records_to_insert = [
                            (
                                row.get('objectid'), row.get('cad_evnt_id'),
                                row.get('create_date')[:10], row.get('incident_date')[:10], row.get('incident_time'),
                                row.get('nypd_pct_cd'), row.get('boro_nm'), row.get('patrl_boro_nm'),
                                row.get('geo_cd_x'), row.get('geo_cd_y'), row.get('radio_code'),
                                row.get('typ_desc'), row.get('cip_jobs'), row.get('add_ts'),
                                row.get('disp_ts'), row.get('arrivd_ts'), row.get('closng_ts'),
                                row.get('latitude'), row.get('longitude')                          
                            ) for row in data
                        ]

                        execute_batch(curs,
                            """
                            INSERT INTO tb_call_data (objectid, cad_evnt_id, create_date, incident_date, incident_time,
                            nypd_pct_cd, boro_nm, patrl_boro_nm, geo_cd_x, geo_cd_y, radio_code, typ_desc, cip_jobs, add_ts,
                            disp_ts, arrivd_ts, closng_ts, latitude, longitude)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, records_to_insert)
                        conn.commit()

                        success = True  # Mark as success to exit the loop
                    elif response.status_code == 408:
                        print(f"Request timeout (HTTP 408) for skip={skip}, retrying attempt {attempt+1}/{max_attempts}.")
                        attempt += 1
                        time.sleep(5 * attempt)  # Exponential back-off
                    else:
                        print(f"Request failed with status code: {response.status_code} for skip={skip}, retrying attempt {attempt+1}/{max_attempts}.")
                        attempt += 1
                        time.sleep(2 ** attempt)  # Exponential back-off for other errors
                except Exception as e:
                    print(f"Exception occurred for skip={skip}: {e}, retrying attempt {attempt+1}/{max_attempts}.")
                    attempt += 1
                    time.sleep(2 ** attempt)  # Exponential back-off for exceptions
    finally:
        pool.putconn(conn)

# Main Execution Logic
def main():
    # Assuming the total records count is hardcoded or successfully fetched
    len_url = 'https://data.cityofnewyork.us/resource/d6zx-ckhd.json?$select=count(*)'
# using requests to store the response
    response = requests.get(len_url)
    #getting the json outpu
    data = response.json()
    # the response is a dictionary with one entry so we index to 0 the first one then call for the key value
    total_records = int(data[0]['count'])  # Update this if you dynamically fetch the count
    num_requests = math.ceil(total_records / RECORDS_PER_REQUEST)
    print(f"Total records to fetch: {total_records}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        futures = []
        for skip in range(0, total_records, RECORDS_PER_REQUEST):
            future = executor.submit(fetch_and_insert_data, skip)
            futures.append(future)

        # Process futures as they complete
        processed_records = 0
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                processed_records += RECORDS_PER_REQUEST
                if processed_records >= 5000000:
                    print(f"Processed approximately {processed_records} records, pausing for {SLEEP_INTERVAL} seconds...")
                    time.sleep(SLEEP_INTERVAL)
                    processed_records = 0  # Reset counter after sleeping
            except Exception as e:
                print(f"An error occurred: {e}")

if __name__ == '__main__':
    main()
    print("Data fetching and insertion tasks have completed.")

