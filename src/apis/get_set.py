import requests, os

class Forecast:
    def set_forecast(self, token_remc, json_data, owner_id, forecast_type, module):
        """
        Sends a POST request to save forecast data to an API endpoint.

        Args:
            token_remc (dict): Dictionary containing access token information.
            data_db (str): Data to be saved.
            owner_id (str): ID of the owner.
            forecast_type (str): Type of forecast.
            prediction_datetime (str): Datetime of the forecast.
            revision (str): Revision number.
            module (str): Optional parameter for module level.

        Returns:
            dict: JSON response from the API.
        """
        
        api_endpoint = os.getenv('remc_base_url') + 'setForecast'

        params = {
            'token': token_remc['access_token'],
            'data': json_data,
            'owner_id': owner_id,
            'type': str(forecast_type),
            'level': module
        }

        r = requests.post(url=api_endpoint, data=params)
        print(str(r))  
        return r.json()