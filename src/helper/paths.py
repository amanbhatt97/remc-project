"""
Path definitions for various directories/files in the project.

Author: Aman Bhatt
"""

from pathlib import Path
import sys, os

from dotenv import load_dotenv
load_dotenv()

PROJECT_PATH = os.getenv('PROJECT_DIR')
sys.path.append(PROJECT_PATH)


class ProjectPaths:
    def __init__(self, parent_directory):
        """
        Initialize ProjectPaths class.

        Args:
            parent_directory (Path): The parent directory of the project.
        """
        # Store the parent directory
        self.parent_directory = parent_directory

        # Define paths for various directories
        self.src = os.path.join(parent_directory, 'src')  # Source scripts
        self.deploy = os.path.join(parent_directory, 'deploy')  # Forecasting/reports scripts
        self.logs = os.path.join(parent_directory, 'logs')       # Logs
        self.models = os.path.join(parent_directory, 'models')   # saved models 
        self.forecasts = os.path.join(parent_directory, 'forecasts')  # forecast files
        self.data = os.path.join(parent_directory, 'data')  # data files
        self.config = os.path.join(parent_directory, 'config')  # configuration related files 
         
        # Create directories if they do not exist
        self._create_directories()


    def _create_directories(self):
        """
        Create necessary directories if they do not exist.
        """
        directories = [
            self.src, self.deploy, self.logs, self.models,
            self.forecasts, self.data, self.config
        ]

        # Create directories if they do not exist
        for directory in directories:
            os.makedirs(directory, exist_ok=True)


# create an instance of the ProjectPaths class
project_paths = ProjectPaths(PROJECT_PATH)

# model path
MODELS_PATH = os.path.join(PROJECT_PATH, 'models')

# dam forecast path
FORECAST_PATH = project_paths.forecasts
VSTF_FORECAST_PATH = os.path.join(project_paths.forecasts, 'VSTF')
IND_FORECAST_PATH = os.path.join(project_paths.forecasts, 'IND')
DA_FORECAST_PATH = os.path.join(project_paths.forecasts, 'DA')
WA_FORECAST_PATH = os.path.join(project_paths.forecasts, 'WA')

# logs path
LOGS_PATH = project_paths.logs


# data path
DATA_PATH = project_paths.data
RAW_DATA_PATH = os.path.join(project_paths.data, 'raw_data') 
PROCESSED_DATA_PATH = os.path.join(project_paths.data, 'processed_data') 

# token path
TOKEN_PATH = os.path.join(PROJECT_PATH, 'config')
CONFIG_PATH = os.path.join(PROJECT_PATH, 'config')