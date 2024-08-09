# Dependencies
from datetime import *
import sys
import time
import os

# set timezone
import time
os.environ['TZ'] = 'Asia/Calcutta'
time.tzset()

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

PROJECT_PATH = os.getenv('PROJECT_DIR')
sys.path.append(PROJECT_PATH)

from src.apis.tokens import APIClient
from src.helper.paths import CONFIG_PATH
from src.helper.utils import load_pickle, save_pickle
apis = APIClient()


class misc:
    
    def __init__(self):
        pass
    
    def saved_solar_plants_ids(self):
        try:
            df = load_pickle(CONFIG_PATH, 'solar_plants_info')
            df['plant_id'] = df['plant_id'].astype('int') 
            df = list(set(df['plant_id']))
            df.sort() 
            return df
        except:
            return self.get_solar_plant_ids() 
    
    def solar_plants_info(self):
        try:
            df = apis.get_plant_info()
            df = df.loc[df.plant_type == 'Solar']
            save_pickle(df, CONFIG_PATH, 'solar_plants_info') 
        except Exception as e:
            print(f'Error in fetching solar plants info: {str(e)}')
            print('Loading saved plant_info details...')
            df = load_pickle(CONFIG_PATH, 'solar_plants_info')   
        return df
        
    def get_avc(self, plant_id):
        # df = self.solar_plants_info()
        df = load_pickle(CONFIG_PATH, 'solar_plants_info')
        avc = float(df.loc[df.plant_id == int(plant_id), 'avc'])
        return avc
    
    def get_solar_plant_ids(self):
        try:
            df = self.solar_plants_info() 
            df['plant_id'] = df['plant_id'].astype('int') 
            df = list(set(df['plant_id']))
            df.sort()
            return df
        except Exception as e:
            print(f'Error in get_soler_plant_ids: {str(e)}')
        
    def prediction_date_and_time(self):
        curr_datetime = datetime.now()
        prediction_datetime = datetime(curr_datetime.year, curr_datetime.month, curr_datetime.day,
                                       curr_datetime.hour, 15*(curr_datetime.minute // 15)
                                       )

        return prediction_datetime
        
        
class revisions:

    def __init__(self, prediction_datetime):
        self.prediction_datetime = prediction_datetime

    def intraday_revision(self):
        """Calculate revision based on prediction datetime."""
        # Define constants
        time_revision = '06:00:00'
        FMT = '%H:%M:%S'
        REVISION_INTERVAL_MINUTES = 90
        
        # Get the prediction time in the specified format
        prediction_time = self.prediction_datetime.strftime(FMT)
        
        # Convert time_revision to datetime object and adjust for buffer
        revision1_time = datetime.strptime(time_revision, FMT)
        time_revision_dt = datetime.strptime(time_revision, FMT) - timedelta(minutes=45)
        
        # Determine current time
        current_time = datetime.strptime(prediction_time, FMT)
        
        # Calculate revision based on current time compared to revision time
        if current_time < revision1_time:
            revision = 1
        else:
            time_difference = current_time - time_revision_dt
            revision = (time_difference.seconds // (60 * REVISION_INTERVAL_MINUTES)) + 2

        return revision
    
    
    def vstf_revision(self):
        # Time before which revision 0 will occur
        time_revision = '06:00:00'
        FMT = '%H:%M:%S'

        # Get the prediction time in the specified format
        prediction_time = self.prediction_datetime.strftime(FMT)
        time_now = prediction_time

        revision1_time = datetime.strptime(time_revision, FMT)

        # Store the original time_revision as a datetime object 
        time_revision = datetime.strptime(time_revision, FMT) - timedelta(minutes=45)

        # If current time(time at which script runs) < 6:00 am -> revision will be 1, else: revision will be calculated
        if time_revision >= datetime.strptime(time_now, '%H:%M:%S'):
            revision = 1
        else:
            tdelta = datetime.strptime(time_now, FMT) - time_revision
            revision = (tdelta.seconds // (60 * 90)) + 2

        return revision
    
    
    def day_ahead_revision(self): 
        # Time before which revision 0 will occur
        time_revision = '06:00:00'
        FMT = '%H:%M:%S'

        # Get the prediction time in the specified format
        prediction_time = self.prediction_datetime.strftime(FMT)
        time_now = prediction_time

        # Store the original time_revision as a datetime object 
        time_revision = datetime.strptime(time_revision, FMT)

        # If current time(time at which script runs) < 6:00 am -> revision will be 0, else: revision will be 1
        if time_revision >= datetime.strptime(time_now, '%H:%M:%S'):
            revision = 0  
        else:
            revision = 1  
            
        return revision
