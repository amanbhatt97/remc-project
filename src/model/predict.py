import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

from src.helper.paths import MODELS_PATH, IND_FORECAST_PATH
from src.helper.utils import load_pickle  
from src.data.process_data import DataProcessor
from src.helper.support import misc

class EMWA_Predict:
    def __init__(self):
        pass
    
    # def load_model(self, plant_id):
    #     self.model_path = os.path.join(MODELS_PATH, 'ewma_models', 'intraday')
    #     # Load the trained model for a specific plant_id
    #     model_file = os.path.join(self.model_path, f'intraday_model_{plant_id}.csv')
    #     if os.path.exists(model_file):
    #         model_df = pd.read_csv(model_file)
    #         return model_df
    #     else:
    #         raise FileNotFoundError(f"Model file 'intraday_model_{plant_id}.csv' not found.")
    
    def predict(self, owner_id, plant_id, model_type, revision):
        
        try:
            # loading model data
            model_file = os.path.join(MODELS_PATH, 'ewma_models', f'{model_type}', f'{model_type}_model_{plant_id}.csv')
            # if os.path.exists(model_file):
            model_df = pd.read_csv(model_file)
            
            # Generate datetime entries for today with 15-minute intervals
            now = datetime.now()
            today = datetime(now.year, now.month, now.day)
            time_blocks = 96  # Number of 15-minute intervals in a day (24 hours * 4)

            date_range = [today + timedelta(minutes=15*i) for i in range(time_blocks)]
            if model_type == 'day_ahead':
                date_range = [today + timedelta(minutes=15*i) + timedelta(days=1) for i in range(time_blocks)]
            df_pred = pd.DataFrame(date_range, columns=['datetime'])
            df_pred = DataProcessor().add_time_block_column(df_pred)
            
            # Round datetime to nearest 15-minute interval
            df_pred['datetime'] = df_pred['datetime'].dt.round('15min')
            
            df_pred = pd.merge(df_pred, model_df, on='tb', how='left')
            df_pred = df_pred[['datetime', 'power']]
            df_pred = df_pred.rename(columns = {'power': 'forecast'})
            
            df_pred['owner_id'] = owner_id
            df_pred['plant_id'] = plant_id 
            df_pred['revision'] = revision
            
            df_pred = df_pred[['owner_id', 'plant_id', 'datetime', 'revision', 'forecast']]
            
            if plant_id != 'aggregated' :
                avc = misc().get_avc(plant_id)
                df_pred.loc[df_pred.forecast > avc, 'forecast'] = avc
            df_pred = df_pred.round(2)
            
            # df_pred.to_csv(os.path.join(IND_FORECAST_PATH, f'intraday_forecast_{plant_id}.csv'))
            print(f'Prediction done for plant-id {plant_id}')
            return df_pred
        
        except FileNotFoundError:
            print(f"model for plant-id {plant_id} does not exist. Skipping...")
        
        
        
        