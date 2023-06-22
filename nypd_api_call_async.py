
import psycopg2
import asyncio
import aiohttp
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

#initiate async
async def download_and_insert(url):
# create a client session
    async with aiohttp.ClientSession() as session:
        # make a request
        response = await session.get(url)

    # parse the data 
    data = await response.json()
    # Check for errors
    if response.status != 200:
        raise Exception("Error downloading data: {}".format(response.status))

    # Parse the data
    data = await response.json()

    # Insert the data into the database
    with conn, conn.cursor() as curs:
        rows_to_insert = []
        for row in data:
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
                "INSERT INTO sch_nypd_calls_tables.tb_call_data (__id, objectid, cad_evnt_id, create_date, incident_date, incident_time, nypd_pct_cd, boro_nm, patrl_boro_nm, geo_cd_x, geo_cd_y, radio_code, typ_desc, cip_jobs, add_ts, disp_ts, arrivd_ts, closng_ts, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                rows_to_insert
                )
        curs.commit()

async def main():
    should_continue = True
    skip = 1

    # Create a semaphore to limit the number of concurrent requests
    sem = asyncio.Semaphore()
    # Loop until there is no more data
    
    while should_continue:
        # Get the next set of data
        url_with_skip = f'{url}?$skip={skip}'
        async with sem:
            data = await download_and_insert(url_with_skip)
            if len(data) == 0:
                should_continue = False
            else:
            # Increase the skip parameter by the number of rows retrieved in the previous request
                skip += len(data)
       
if __name__ == "__main__":
    #run asyncio main function
    asyncio.run(main())
