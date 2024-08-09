import os
import requests
import pandas as pd
from dotenv import load_dotenv
import json
from datetime import datetime, timedelta

# Load environment variables from a .env file if present
load_dotenv()

from src.helper.paths import CONFIG_PATH
from src.helper.utils import save_pickle, load_pickle

class APIClient:
    def __init__(self):
        self.cc_base_url = os.getenv('cc_base_url')
        self.remc_base_url = os.getenv('remc_base_url')
        self.cc_username = os.getenv('cc_username')
        self.cc_password = os.getenv('cc_password')
        self.remc_email = os.getenv('remc_email')
        self.remc_password = os.getenv('remc_password')
        self.cc_token_file = os.path.join(CONFIG_PATH, 'token_cc.json')
        self.remc_token_file = os.path.join(CONFIG_PATH, 'token_remc.json')
        self.cc_token = self.load_token(self.cc_token_file)
        self.remc_token = self.load_token(self.remc_token_file)

    def load_token(self, token_file):
        """Load token from JSON file if it exists and is valid."""
        if os.path.exists(token_file):
            with open(token_file, 'r') as file:
                token_data = json.load(file)
                # Check if token is still valid
                if 'expires_at' in token_data and datetime.strptime(token_data['expires_at'], '%Y-%m-%dT%H:%M:%S') > datetime.now():
                    return token_data
        return None

    def save_token(self, token_file, token_data):
        """Save token to JSON file."""
        with open(token_file, 'w') as file:
            json.dump(token_data, file)

    def get_cc_token(self):
        """Get Climate Connect token. Generate new token if expired or not available."""
        if not self.cc_token:
            api_endpoint = f"{self.cc_base_url}get-token"
            headers = {'Content-Type': 'application/json'}
            params = {'username': self.cc_username, 'password': self.cc_password}
            response = requests.post(url=api_endpoint, headers=headers, params=params)
            response.raise_for_status()  # Raise an error for bad responses
            token_data = response.json()
            self.save_token(self.cc_token_file, token_data)
            self.cc_token = token_data
        return self.cc_token

    def get_remc_token(self):
        """Get REMC token. Generate new token if expired or not available."""
        #if not self.remc_token or datetime.strptime(self.remc_token['expires_at'], '%Y-%m-%dT%H:%M:%S') <= datetime.now():
        api_endpoint = f"{self.remc_base_url}get-token"
        headers = {'Content-Type': 'application/json'}
        params = {'email': self.remc_email, 'password': self.remc_password}
        response = requests.post(url=api_endpoint, headers=headers, params=params)
        response.raise_for_status()  # Raise an error for bad responses
        token_data = response.json()
        token_data['expires_at'] = (datetime.now() + timedelta(seconds=token_data['expires_in'])).strftime('%Y-%m-%dT%H:%M:%S')
        self.save_token(self.remc_token_file, token_data)
        self.remc_token = token_data
        return self.remc_token

    def get_plants(self, token_remc):
        """Get plants details using REMC token."""
        
        api_endpoint = f"{self.remc_base_url}getPlant"
        headers = {'Content-Type': 'application/json'}
        params = {'token': token_remc['access_token']}
        response = requests.post(url=api_endpoint, headers=headers, params=params)
        response.raise_for_status()  # Raise an error for bad responses
        return response.json()
    
    def get_plant_info(self):
        """Get list of plants using REMC token."""
        # plants_info_path = os.path.join(CONFIG_PATH, 'plant_info')
        # if os.path.exists(plants_info_path):
        #     return load_pickle(CONFIG_PATH, 'plant_info')
        
        # if not self.remc_token:
        #     raise ValueError("REMC token is not set. Call get_remc_token() first.")
        
        plants = self.get_plants(self.remc_token)
            
        plant_details = pd.DataFrame(plants['data'].values())
        return plant_details

    
