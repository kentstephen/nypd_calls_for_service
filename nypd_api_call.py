import requests
import psycopg2
import time
from datetime import datetime
CurrentDateAndTime = datetime.now()
#importing our postgres creds
from postgres_config import POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER

url = "https://data.cityofnewyork.us/api/odata/v4/d6zx-ckhd"

# Connect to the database
conn = psycopg2.connect(
    host = POSTGRES_HOST,
    port = POSTGRES_PORT,
    dbname = POSTGRES_DB,
    user = POSTGRES_USER,
    password = POSTGRES_PASSWORD
)
#setting the start time for the program
start_time = time.time()
#setting up so we know how much time has elapsed as the progrma rungs
currentTime = CurrentDateAndTime.strftime("%I:%M:%p")
print(f"The program began at {currentTime}")
def format_time(elapsed_time):
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds"

# Set an initial value for the skip parameter
skip = 0


# Add the skip parameter to the URL, this will allow us to skip through rows because you can't call the api one time for everything
url_with_skip = f'{url}?$skip={skip}'
# Make the API request
response = requests.get(url_with_skip)
#the response
data = response.json()
print("getting first batch")
# Extract the data in json format
data_dict = data['value']
# Write the data to the newly created table in the database
no_data_flag = False
entering_the_loop = True

total_rows_inserted = 0
# the first few print statments will come quicker than the last few, these show how well you're doing
milestones = [1000, 10000, 50000, 100000, 500000, 1000000, 1500000, 2000000, 5000000, 10000000, 15000000, 20000000, 25000000, 30000000]

while not no_data_flag: #as long as this no_data_flag is false the program will continue, when len(data_dict) == 0 it will exit
    if entering_the_loop: #this runs so we know we're entering our while loop
        print("Entering the loop... this could take a while")
        entering_the_loop = False
    
    # check if ther is no more data
    if len(data_dict) == 0:
        with conn, conn.cursor() as curs:
            curs.execute("SELECT COUNT(*) FROM sch_nypd_calls_tables.tb_call_data") #return the final count
            row_count = curs.fetchone()[0]
            CurrentDateAndTime = datetime.now() #get the time and elapsed time
            endTime = CurrentDateAndTime.strftime("%I:%M:%p")
            elapsed_time = time.time() - start_time
            formattted_time = format_time(elapsed_time)
            print(f"\nAwesome, you did it! At {endTime}: {formattted_time} after the program started, {row_count} rows were inserted into your table.")
            curs.commit()
            no_data_flag = True

    with conn, conn.cursor() as curs:
        rows_to_insert = []
        for row in data_dict: #insert the data into prostgres
            rows_to_insert.append((
                row['__id'], row['objectid'], row['cad_evnt_id'], 
                row['create_date'], row['incident_date'], row['incident_time'], 
                row['nypd_pct_cd'], row['boro_nm'], row['patrl_boro_nm'], 
                row['geo_cd_x'], row['geo_cd_y'], row['radio_code'], 
                row['typ_desc'], row['cip_jobs'], row['add_ts'], 
                row['disp_ts'], row['arrivd_ts'], row['closng_ts'], 
                row['latitude'], row['longitude']
            ))
        curs.executemany(       
                "INSERT INTO sch_nypd_calls_tables.tb_call_data (__id, objectid, cad_evnt_id, create_date, incident_date, incident_time, nypd_pct_cd, boro_nm, patrl_boro_nm, geo_cd_x, geo_cd_y, radio_code, typ_desc, cip_jobs, add_ts, disp_ts, arrivd_ts, closng_ts, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                rows_to_insert
                )
        total_rows_inserted += len(rows_to_insert)
        if total_rows_inserted in milestones:
            elapsed_time = time.time() - start_time
            formattted_time = format_time(elapsed_time)
            print(f"{formattted_time} after the program began, {total_rows_inserted} rows have been inserted into Postgres table")
    # Increase the skip parameter by the number of rows retrieved in the previous request
    skip += len(data_dict)
    # Fetch the next set of data
    url_with_skip = f'{url}?$skip={skip}'
    response = requests.get(url_with_skip)
    try:
        data = response.json()
        if 'value' not in data: # got an error that value wasn't in the response so we tell it to keep going anyway
            continue
        data_dict = data['value']
    #this will handle any errors but will break the loop and end the program
    except KeyError:
        print("Error: there was an error in the response, which is:")
        print(response.content)
        continue

