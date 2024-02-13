import psycopg2
from postgres_config import POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER
from datetime import datetime
# Connect to the database
conn = psycopg2.connect(
    host = POSTGRES_HOST,
    port = POSTGRES_PORT,
    dbname = POSTGRES_DB,
    user = POSTGRES_USER,
    password = POSTGRES_PASSWORD
)

with conn, conn.cursor() as curs:
    curs.execute("SELECT MAX(incident_date), MIN(incident_date) FROM tb_call_data")
    result = curs.fetchone()
end_date = (result[0].strftime('%Y-%m-%d'))
start_date = (result[1].strftime('%Y-%m-%d'))

print('End date', end_date)
print('Start date', start_date)