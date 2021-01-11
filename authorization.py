import json
import os
import webbrowser
import requests
from config import *

SCOPES = ['https://www.googleapis.com/auth/photoslibrary',
          'https://www.googleapis.com/auth/photoslibrary.edit.appcreateddata',
          'https://www.googleapis.com/auth/photoslibrary.sharing']


class Authorization:
    def __init__(self):
        try:
            with open(IDENTITY_FILE_PATH) as file_data:
                self.identity_data = json.load(file_data)['installed']
        except OSError as err:
            logging.error(f'Error while reading {IDENTITY_FILE_PATH}, {err}')
            exit(1)
        scopes_for_uri = ''
        for scope in SCOPES:
            scopes_for_uri += scope+'%20'
        self.url = f"{self.identity_data['auth_uri']}?scope={scopes_for_uri}&response_type=code&" \
                   f"redirect_uri={self.identity_data['redirect_uris'][0]}&client_id={self.identity_data['client_id']}"
        if not os.path.exists(OAUTH2_FILE_PATH):
            auth_result = self.authenticate()
            if auth_result != 0:
                logging.error("Unexpected error.")
                exit(2)
        with open(OAUTH2_FILE_PATH) as f:
            self.oauth2_data = json.load(f)
        self.access_token = self.oauth2_data['access_token']

    def authenticate(self) -> int:
        print(f"If you do not have local browser please visit url: {self.url}")
        webbrowser.open(self.url, new=0, autoraise=True)
        code = input("Please enter the code: ")
        data = {'code': code,
                'client_id': self.identity_data['client_id'],
                'client_secret': self.identity_data['client_secret'],
                'redirect_uri': self.identity_data['redirect_uris'][0],
                'grant_type': 'authorization_code'}
        response = requests.post(self.identity_data['token_uri'], data=data)
        if response.status_code != 200:
            logging.error(f"Not authenticated. http code: {response.status_code}, response: {response.text}.")
            return 1
        try:
            with open(OAUTH2_FILE_PATH, 'w') as f:
                json.dump(response.json(), f)
        except OSError as err:
            logging.error(f'Error while writing {OAUTH2_FILE_PATH}, {err}')
            return 2
        logging.info("Authenticated successfully.")
        return 0

    def get_access_token(self):
        if os.path.exists(ACCESS_TOKEN_FILE):
            with open(ACCESS_TOKEN_FILE) as f:
                self.access_token = json.load(f)['access_token']
        elif os.path.exists(OAUTH2_FILE_PATH):
            with open(OAUTH2_FILE_PATH) as f:
                self.access_token = json.load(f)['access_token']
        else:
            logging.error("Not authenticated.")
            return 1
        return self.access_token

    def refresh_access_token(self):
        values = {'client_id': self.identity_data['client_id'],
                  'client_secret': self.identity_data['client_secret'],
                  'refresh_token': self.oauth2_data['refresh_token'],
                  'grant_type': 'refresh_token'}
        response = requests.post(self.identity_data['token_uri'], data=values)
        with open(ACCESS_TOKEN_FILE, 'w') as f:
            json.dump(response.json(), f)
        logging.info('Token has been refreshed.')
