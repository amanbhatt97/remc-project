import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from functools import partial

# set timezone
import time
os.environ['TZ'] = 'Asia/Calcutta'
time.tzset()

# Load environment variables from a .env file if present
load_dotenv()

from src.helper.paths import RAW_DATA_PATH
from src.apis.tokens import APIClient
from src.helper.utils import save_pickle

apis = APIClient()

class DataPrep:
    def __init__(self, token):
        self.remc_base_url = os.getenv('remc_base_url')
        self.token_access = token['access_token']
        
    def get_solar_data(self, plant_id, start_date, prediction_date):
        try:
            if plant_id == 'aggregated':
                df = pd.DataFrame(self.get_actual_aggregated_avg(start_date, prediction_date)['data'])
                df = df.sort_values(by='utc_datetime')
                # save_pickle(df, RAW_DATA_PATH, f'solar_plant_{plant_id}')
                return df
            else:
                dates = pd.date_range(start_date, prediction_date).date

                fetch_func = partial(self.fetch_data_for_date, plant_id)
                with ThreadPoolExecutor() as executor:
                    df_list = list(executor.map(fetch_func, dates))

                df = pd.concat(df_list, ignore_index=True)
                df = df.sort_values(by='utc_datetime')
                print(f'Data fetched for plant-id: {plant_id}')
                return df
        except Exception as e:
            print(f"Error fetching solar data for plant-id {plant_id}: {e}")
            return None

    def fetch_data_for_date(self, plant_id, date):
        try:
            data = pd.DataFrame(self.get_solar_actual(plant_id, date)['data'])
            return data
        except Exception as e:
            print(f"Error fetching data for plant-id {plant_id} on date {date}: {e}")
            return pd.DataFrame()  # Return an empty DataFrame in case of error

    def get_solar_actual(self, plant_id, date):
        try:
            headers = {'Content-Type': 'application/json'}
            api_endpoint = f'{self.remc_base_url}getActual'
            params = {'token': self.token_access, 'date': date, 'plant_id': plant_id}
            r = requests.post(url=api_endpoint, headers=headers, params=params).json()
            return r
        except Exception as e:
            print(f"Error in get_solar_actual for plant-id {plant_id} on date {date}: {e}")
            return {}

    def get_actual_aggregated_avg(self, from_date, to_date):
        try:
            api_endpoint = f"{self.remc_base_url}getActualAggregateAvg"
            params = {'token': self.token_access, 'from_date': from_date, 'to_date': to_date, 'type': 'Solar'}
            response = requests.get(url=api_endpoint, headers={'Content-Type': 'application/json'}, params=params).json()
            return response
        except Exception as e:
            print(f"Error in get_actual_aggregated_avg from {from_date} to {to_date}: {e}")
            return {}

    def get_solar_data_for_plants(self, plant_list, start_date, prediction_date):
        try:
            fetch_func = partial(self.get_solar_data, start_date=start_date, prediction_date=prediction_date)
            with ThreadPoolExecutor() as executor:
                executor.map(fetch_func, plant_list)
        except Exception as e:
            print(f"Error fetching solar data for plant list: {e}")
            
    def update_solar_data(self, plant_id):
        try:
            file_path = os.path.join(RAW_DATA_PATH, f'solar_plant_{plant_id}')
            if os.path.exists(file_path):
                df_existing = pd.read_pickle(file_path)
                df_existing['utc_datetime'] = pd.to_datetime(df_existing['utc_datetime'])
                last_date = df_existing['utc_datetime'].max().date()
                start_date = last_date + timedelta(days=0)
                end_date = datetime.now().date()
                
                if start_date <= end_date:
                    print(f"Fetching data for plant-id {plant_id} from {start_date} to {end_date}")
                    new_data = self.get_solar_data(plant_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
                    if new_data is not None and not new_data.empty:
                        new_data['utc_datetime'] = pd.to_datetime(new_data['utc_datetime'])
                        df_updated = pd.concat([df_existing, new_data], ignore_index=True)
                        df_updated = df_updated.drop_duplicates(subset='utc_datetime')
                        df_updated = df_updated.sort_values(by='utc_datetime')
                        save_pickle(df_updated, RAW_DATA_PATH, f'solar_plant_{plant_id}')
                        print(f"Data updated and saved for plant-id: {plant_id}")
                    else:
                        print(f"No new data to update for plant-id: {plant_id}")
                else:
                    print(f"No new data to fetch. Last date in the dataset is up-to-date for plant-id: {plant_id}")
            else:
                print(f"No existing data found for plant-id: {plant_id}. Fetching the plants data from 2024-01-01 onwards.")
                df = self.get_solar_data(plant_id, '2024-01-01', datetime.now().strftime('%Y-%m-%d'))  # Adjust start date as needed
                save_pickle(df, RAW_DATA_PATH, f'solar_plant_{plant_id}')
                print(f"Data updated and saved for plant-id: {plant_id}") 
        except Exception as e:
            print(f"Error updating solar data for plant-id {plant_id}: {e}")
            
    
    def update_solar_data_for_plants(self, plant_list):
        try:
            with ThreadPoolExecutor() as executor:
                executor.map(self.update_solar_data, plant_list)
        except Exception as e:
            print(f"Error updating solar data for plant list: {e}")