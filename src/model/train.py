import pandas as pd
import numpy as np
from datetime import timedelta, datetime
import os

from src.helper.paths import MODELS_PATH, PROCESSED_DATA_PATH
from src.helper.utils import load_pickle

# set timezone
import time
os.environ['TZ'] = 'Asia/Calcutta'
time.tzset()

class EMWA_Train:
    def __init__(self):
        self.model = None
    
    def train_model(self, plant_id, model_type, days: int = 9, alpha: float = 0.3, tbs: int = 5, weight_recent: float = 0.7):
        
        try:
            self.model_path = os.path.join(MODELS_PATH, 'ewma_models', f'{model_type}')
            
            # Load processed plant data
            data = load_pickle(PROCESSED_DATA_PATH, f'processed_solar_plant_{plant_id}')
        
            # Ensure datetime column is in datetime format
            data['datetime'] = pd.to_datetime(data['datetime'])
            
            # Get the last date in the data
            last_date = data['datetime'].max().date()
            last_tb = data['tb'].iloc[-1]
            
            # Filter data for the last `days` days
            start_date = last_date - timedelta(days=days)
            recent_data = data[data['datetime'].dt.date >= start_date]
            recent_tbs = recent_data.tail(tbs)
            
            # Calculate the exponentially weighted moving average power for each time block
            ewma_days = recent_data.groupby('tb')['power'].apply(lambda x: x.ewm(alpha=alpha).mean().iloc[-1]).reset_index()
            ewma_recent = recent_tbs['power'].mean()
            
            last_datetime = data['datetime'].max() 
            if last_datetime > datetime.now() + timedelta(hours = -2.5):
                # print('Taking recent tbs...')
                # Update non-zero power values with the weighted average
                ewma_days['power'] = np.where(
                    # (ewma_days['power'] != 0) & (ewma_days['tb'] < last_tb+10) & (ewma_days['tb'] > last_tb-10),
                    (ewma_days['power'] != 0),
                    (1 - weight_recent) * ewma_days['power'] + weight_recent * ewma_recent,
                    ewma_days['power']
                    )
                
            # Store the model (EWMA power for each time block)
            self.model = ewma_days
            
            # Ensure the directory exists
            os.makedirs(self.model_path, exist_ok=True)
            
            # Save the model
            self.model.to_csv(os.path.join(self.model_path, f'{model_type}_model_{plant_id}.csv'), index=False)
            
            print(f"Model for plant {plant_id} has been trained using days={days} and alpha={alpha}.")
        
        except FileNotFoundError:
            print(f"Model for {plant_id}' not found. Skipping...")
    