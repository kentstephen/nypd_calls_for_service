
import requests
import pandas as pd
from postgres_config import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER

url = 'https://archive-api.open-meteo.com/v1/archive?latitude=40.71&longitude=-74.01&start_date=2018-01-01&end_date=2021-12-31&daily=temperature_2m_max,temperature_2m_min&timezone=America%2FNew_York&temperature_unit=fahrenheit'

#get the data from the url
response = requests.get(url)
response_dict = response.json() 
df = pd.json_normalize(response_dict) # turn it to a dataframe and normalize it
selected_columns = ['daily.time', 'daily.temperature_2m_max', 'daily.temperature_2m_min'] # we want these
new_df = df[selected_columns] # only use the columns we want
new_df = new_df.explode(selected_columns) #give each value its own row
new_df.reset_index(drop=True, inplace=True) #drop the old index 
# Print the new DataFrame
#print(new_df)
# new_df.to_csv('ny_weather.csv', index = False, encoding='utf-8')
#match the dataframe's columns with our pg column
column_mapping = {
    'daily.time': 'date',
    'daily.temperature_2m_max': 'daily_temp_maximum',
    'daily.temperature_2m_min': 'daily_temp_minimum'
}

new_df = new_df.rename(columns=column_mapping)

#connect to the database
from sqlalchemy import create_engine
engine= create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
#insert into postgres
new_df.to_sql('tb_weather_data', engine, if_exists='append', index=False, schema='sch_nypd_calls_tables')
engine.dispose()

