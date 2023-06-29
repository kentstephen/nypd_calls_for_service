import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from A_postgres_config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
# connect to Posgtres Server
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    database='postgres' # here we're using the default postgres database in order to create a new one, that of which we will use going forward
)
# using to create database
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cursor = conn.cursor()

# Check if the database already exists
database_name = "db_nypd_calls_for_service_data"
check_db_query = f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'"
try:
    cursor.execute(check_db_query)
    database_exists = cursor.fetchone()

    if not database_exists:
        # Create the database
        create_db_query = f"CREATE DATABASE {database_name}"
        cursor.execute(create_db_query)
        print("Database created.") # we made it
    else:
        print("Database found") #it's already in there
except psycopg2.Error as e:
    print(f"Error creating or checking database: {e}")
# switch to new database

conn.close() # close first so we can reconnect to the database
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    database=POSTGRES_DB # here we're using the database name defined above instead of the default postgres, this is saved in our postgres_config file
)
#connecting to postgres
cursor = conn.cursor()
# identifying our schema and seeing if it exists and if not to make it
schema_name = 'sch_nypd_calls_tables'
check_schema_query = f"SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema_name}')"
cursor.execute(check_schema_query)
schema_exists = cursor.fetchone()[0]

if not schema_exists:
    # if it doesn't exist we create the schema
    create_schema_query = f"CREATE SCHEMA {schema_name}"
    cursor.execute(create_schema_query)
    print("Schema created!")
else:
    print("Schema found") # we don't need to re create it we don't want duplicates

# Create a new table in the database
cursor = conn.cursor()
table_name = 'tb_call_data'
check_table_query = f"SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{table_name}')"
cursor.execute(check_table_query)
table_exists = cursor.fetchone()[0]

if not table_exists:
    try:
        with conn, conn.cursor() as curs:
            curs.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (__id VARCHAR(30), objectid INTEGER, cad_evnt_id INTEGER, create_date TIMESTAMP, incident_date TIMESTAMP, incident_time VARCHAR(30), nypd_pct_cd INTEGER, boro_nm TEXT, patrl_boro_nm TEXT, geo_cd_x INTEGER, geo_cd_y INTEGER, radio_code VARCHAR(6), typ_desc TEXT, cip_jobs TEXT, add_ts TIMESTAMP, disp_ts TIMESTAMP, arrivd_ts TIMESTAMP, closng_ts TIMESTAMP, latitude NUMERIC, longitude NUMERIC)")
        conn.commit()
        print("Table created.")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
else: 
    print("Calls table already exists")
#close cursor and connection


# Create a new table in the database

# Create the weather data table in the database
cursor = conn.cursor()
# name our table
weather_table_name = 'tb_weather_data'
# query to see if it's already there
check_weather_table_query = f"SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{weather_table_name}')"
cursor.execute(check_weather_table_query)
weather_table_exists = cursor.fetchone()[0] #return a boolean value from the query
#if it's not there we make it, and if it's already there it notifies us
if not weather_table_exists:
    try:
        with conn, conn.cursor() as curs:
            curs.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{weather_table_name} (date DATE PRIMARY KEY, daily_temp_maximum FLOAT, daily_temp_minimum FLOAT)")
        conn.commit()
        print("weather table created.")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
else:
    print("weather table already exists")
#close cursor and connection
cursor.close()
conn.close()