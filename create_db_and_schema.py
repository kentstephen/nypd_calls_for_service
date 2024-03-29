import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from postgres_config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB

def create_database(database_name):
    # connect to Posgtres Server
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database='postgres'
    )
    # using to create database
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = conn.cursor()

    # Check if the database already exists
    check_db_query = f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'"
    try:
        cursor.execute(check_db_query)
        database_exists = cursor.fetchone()

        if not database_exists:
            # Create the database
            create_db_query = f"CREATE DATABASE {database_name}"
            cursor.execute(create_db_query)
            print("Database created.")
        else:
            print("Database found")
    except psycopg2.Error as e:
        print(f"Error creating or checking database: {e}")

    # close first so we can reconnect to the database
    conn.close()
    return database_name

def create_schema(database_name, schema_name):
    # switch to new database
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=database_name
    )

    cursor = conn.cursor()
    # identifying our schema and seeing if it exists and if not to make it
  

    # Create a new table in the database
    cursor = conn.cursor()
    table_name = 'tb_call_data'
    check_table_query = f"SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_name = '{table_name}')"
    cursor.execute(check_table_query)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        try:
            with conn, conn.cursor() as curs:
                curs.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (objectid INTEGER, cad_evnt_id INTEGER, create_date DATE, incident_date DATE, incident_time VARCHAR(50), nypd_pct_cd INTEGER, boro_nm TEXT, patrl_boro_nm TEXT, geo_cd_x INTEGER, geo_cd_y INTEGER, radio_code VARCHAR(6), typ_desc TEXT, cip_jobs TEXT, add_ts TIMESTAMP, disp_ts TIMESTAMP, arrivd_ts TIMESTAMP, closng_ts TIMESTAMP, latitude DECIMAL(8,6), longitude DECIMAL(9,6))")
            conn.commit()
            print("Table created.")
        except psycopg2.Error as e:
            print(f"Error creating table: {e}")
    else:
        print("Calls table already exists")

    # Create the weather data table in the database
    cursor = conn.cursor()
    # name our table
    weather_table_name = 'tb_weather_data'
    # query to see if it's already there
    check_weather_table_query = f"SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_name = '{weather_table_name}')"
    cursor.execute(check_weather_table_query)
    weather_table_exists = cursor.fetchone()[0]  # return a boolean value from the query
    # if it's not there we make it, and if it's already there it notifies us
    if not weather_table_exists:
        try:
            with conn, conn.cursor() as curs:
                curs.execute(f"CREATE TABLE IF NOT EXISTS {weather_table_name} (date DATE PRIMARY KEY, daily_temp_maximum FLOAT, daily_temp_minimum FLOAT)")
            conn.commit()
            print("weather table created.")
        except psycopg2.Error as e:
            print(f"Error creating table: {e}")
    else:
        print("weather table already exists")
    # close cursor and connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    db_name = create_database(POSTGRES_DB)
    create_schema(db_name, 'sch_nypd_calls_tables')
