# %%
import os, sys
import pandas as pd
# Load environment variables
from dotenv import load_dotenv
load_dotenv()

PROJECT_PATH = os.getenv('PROJECT_DIR')
sys.path.append(PROJECT_PATH)

# ignore warnings
import warnings
warnings.filterwarnings('ignore')

# set timezone
import time
os.environ['TZ'] = 'Asia/Calcutta'
time.tzset()

# %%
# custom modules
from src.apis.tokens import APIClient
from src.data.load_data import DataPrep
from src.data.process_data import DataProcessor
from src.helper.support import misc
from src.helper.paths import CONFIG_PATH

# %%
# instances
apis = APIClient()
clean_data = DataProcessor() 
misc = misc()
# %%
print('Script for fetching data running...')

token_remc = apis.get_remc_token()
print('Token fetched successfully!')

plant_ids = misc.get_solar_plant_ids()
plant_ids.append('aggregated')

# plant_ids = [
            #  42, 46, 52, 57, 59, 71, 79, 93, 94, 115, 116, 187, 198, 243, 249
            #  ]

print(f'Fetching data for plant-ids: {plant_ids}')
# # %%
data = DataPrep(token_remc)

# %%
data.update_solar_data_for_plants(plant_ids)
print('Data fetching done!')
print('------------------------------------')
clean_data.process_multiple_plants(plant_ids)
print('Data processing done!')
print('------------------------------------')
# %%



