import numpy as np
import pandas as pd
from datetime import datetime
import os
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Load environment variables from a .env file if present
load_dotenv()

from src.helper.paths import PROCESSED_DATA_PATH, RAW_DATA_PATH, CONFIG_PATH
from src.helper.utils import load_pickle, save_pickle
from src.helper.support import misc
from src.apis.tokens import APIClient

apis = APIClient()

class DataProcessor:
    def __init__(self):
        pass
    
    def process_raw_data(self, plant_id):
        """
        Process raw solar data for a plant, including renaming columns, time bucket division,
        reindexing, adding time bucket column, handling sunrise/sunset blocks, and applying capacity limit.

        Parameters:
        - df: DataFrame, raw solar data

        Returns:
        - df_processed: DataFrame, processed solar data
        """
        df = load_pickle(RAW_DATA_PATH, f'solar_plant_{plant_id}')
        df = self.rename_columns(df)
        # df = self.time_block_division(df)
        df = self.data_reindex(df)
        df = self.add_time_block_column(df)
        if plant_id != 'aggregated':
            avc = misc().get_avc(plant_id)
            self.apply_capacity_limit(df, avc)
        return df
    
    def rename_columns(self, df):
        """
        Rename columns of the DataFrame.

        Parameters:
        - df: DataFrame, input DataFrame with columns to be renamed

        Returns:
        - df_renamed: DataFrame, DataFrame with renamed columns
        """
        df.rename(columns={'utc_datetime': 'datetime', 'generation': 'power'}, inplace=True)
        return df[['datetime', 'power']]
    
    
    def data_reindex(self, df):
        try:
            df['datetime'] = pd.to_datetime(df['datetime'])  # Ensure 'datetime' column is in datetime format
            df.set_index('datetime', inplace=True)
            
            # Create a new date range for the index based on the min and max dates in the original index
            new_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq='15T')
            
            # Reindex the DataFrame to this new index
            df = df.reindex(new_index, method=None)
            
            # Reassign the new index back to the 'datetime' column
            df['datetime'] = df.index
            
            df.reset_index(drop=True, inplace=True)
            
            return df
        except Exception as e:
            print(f"An error occurred during reindexing: {e}")
            return None
    
    
    def add_time_block_column(self, df):
        """
        Add a time block column ('tb') to the DataFrame.

        Parameters:
        - df: DataFrame, input DataFrame to which 'tb' column will be added

        Returns:
        - df_with_tb: DataFrame, DataFrame with 'tb' column added
        """
        df['tb'] = pd.to_datetime(df['datetime']).apply(lambda x: ((x.hour * 60 + x.minute) // 15) + 1)
        return df
    
    def get_sunrise_sunset_blocks(self, df):
        """
        Determine sunrise and sunset time bucket blocks based on the DataFrame.

        Parameters:
        - df: DataFrame, input DataFrame containing solar data

        Returns:
        - sunrise_block: int, time bucket block for sunrise
        - sunset_block: int, time bucket block for sunset
        """
        temp_df = df.tail(960)
        temp_df = temp_df[temp_df['power'] > 0].copy()
        temp_df['tb'] = self.add_time_block_column(temp_df)['tb']
        
        sunrise_block = min(20, int(temp_df.groupby('datetime').min().tb.mode()[0]))
        sunset_block = max(int(temp_df.groupby('datetime').max().tb.mode()[0]), 80)
        # sunrise_block = int(temp_df.groupby('datetime').tb.min().mode()[0])
        # sunset_block = int(temp_df.groupby('datetime').tb.max().mode()[0])
        
        return sunrise_block, sunset_block
    
    def apply_capacity_limit(self, df, avc):
        """
        Apply capacity limit to the 'power' column of the DataFrame.

        Parameters:
        - df: DataFrame, input DataFrame with 'power' column to be processed
        - capacity: float, maximum capacity limit
        - sunrise_block: int, time bucket block for sunrise
        - sunset_block: int, time bucket block for sunset

        Returns:
        - None (modifies df in place)
        """
        sunrise_block, sunset_block = self.get_sunrise_sunset_blocks(df)
        print(sunrise_block, sunset_block)
        df.loc[(df.tb < sunrise_block) | (df.tb > sunset_block), 'power'] = 0
        df.loc[df.power < 0, 'power'] = np.nan
        df.loc[df.power > avc, 'power'] = avc
    
    def process_single_plant(self, plant_id):
        """
        Process data for a single plant. To be used with parallel processing.

        Parameters:
        - plant_id: int, plant ID

        Returns:
        - plant_id: int, plant ID
        - processed_data: DataFrame or None, processed DataFrame or None if an error occurred
        """
        try:
            print(f"Processing data for plant ID: {plant_id}")
            processed_data = self.process_raw_data(plant_id)
            save_pickle(processed_data, PROCESSED_DATA_PATH, f'processed_solar_plant_{plant_id}')
            return plant_id, processed_data
        except FileNotFoundError:
            print(f"Data for plant ID {plant_id} not found. Skipping...")
            return plant_id, None
        except Exception as e:
            print(f"An error occurred while processing plant ID {plant_id}: {e}. Skipping...")
            return plant_id, None

    def process_multiple_plants(self, plant_ids):
        """
        Process data for multiple plants in parallel.

        Parameters:
        - plant_ids: list of plant IDs

        Returns:
        - processed_data: dict, dictionary with plant IDs as keys and processed DataFrames as values
        """
        processed_data = {}
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.process_single_plant, plant_id) for plant_id in plant_ids]
            for future in as_completed(futures):
                plant_id, data = future.result()
                if data is not None:
                    processed_data[plant_id] = data
                else:
                    print(f"Skipping plant ID: {plant_id} due to processing error.")
        # return processed_data
