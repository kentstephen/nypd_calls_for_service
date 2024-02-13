from select_start_and_end_date import start_date, end_date
import requests
import pandas as pd
from postgres_config import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER

url = f'https://archive-api.open-meteo.com/v1/archive?latitude=40.71&longitude=-74.01&start_date={start_date}&end_date={end_date}&daily=temperature_2m_max,temperature_2m_min&timezone=America%2FNew_York&temperature_unit=fahrenheit'

#get the data from the url
response = requests.get(url)
response_dict = response.json() 
df = pd.json_normalize(response_dict) # turn it to a dataframe and normalize it
selected_columns = ['daily.time', 'daily.temperature_2m_max', 'daily.temperature_2m_min'] # we want these
new_df = df[selected_columns] # only use the columns we want
new_df = new_df.explode(selected_columns) #give each value its own row
new_df.reset_index(drop=True, inplace=True) #drop the old index 

column_mapping = {
    'daily.time': 'date',
    'daily.temperature_2m_max': 'daily_temp_max',
    'daily.temperature_2m_min': 'daily_temp_min'
}

new_df = new_df.rename(columns=column_mapping)

#connect to the database
from sqlalchemy import create_engine
engine= create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
#insert into postgres
new_df.to_sql('tb_weather_data', engine, if_exists='replace', index=False)
engine.dispose()

