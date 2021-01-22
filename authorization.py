import json
import os
import webbrowser
import requests
from config import *
from exceptions import *

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
            raise
        except KeyError:
            logging.error(f"Invalid {IDENTITY_FILE_PATH} file.")
            raise
        scopes_for_uri = ''
        for scope in SCOPES:
            scopes_for_uri += scope+'%20'
        self.url = f"{self.identity_data['auth_uri']}?scope={scopes_for_uri}&response_type=code&" \
                   f"redirect_uri={self.identity_data['redirect_uris'][0]}&client_id={self.identity_data['client_id']}"
        self.access_token = None
        self.refresh_token = None

    def authenticate(self):
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
            raise Exception()
        try:
            with open(OAUTH2_FILE_PATH, 'w') as file:
                json.dump(response.json(), file)
        except OSError as err:
            logging.error(f'Error while writing {OAUTH2_FILE_PATH}, {err}')
            raise
        self.access_token = response.json()['access_token']
        self.refresh_token = response.json()['refresh_token']
        logging.warning("Authenticated successfully.")

    def get_tokens(self):
        try:
            with open(OAUTH2_FILE_PATH) as file:
                oauth_file_data = json.load(file)
                self.refresh_token = oauth_file_data['refresh_token']
                self.access_token = oauth_file_data['access_token']
        except FileNotFoundError:
            logging.warning("Authentication require.")
            raise
        except OSError as err:
            logging.error(f"Fail to read {OAUTH2_FILE_PATH}, {err}")
            raise
        except KeyError as err:
            logging.error(f"File does not contain tokens, {err}")
            raise
        if os.path.exists(ACCESS_TOKEN_FILE):
            with open(ACCESS_TOKEN_FILE) as file:
                file_content = file.read()
                try:
                    self.access_token = json.loads(file_content)['access_token']
                except KeyError:
                    logging.error(f"File {ACCESS_TOKEN_FILE} exists but does not contain the access token, "
                                  f"{file_content}")
                    raise

    def refresh_access_token(self):
        values = {'client_id': self.identity_data['client_id'],
                  'client_secret': self.identity_data['client_secret'],
                  'refresh_token': self.refresh_token,
                  'grant_type': 'refresh_token'}
        response = requests.post(self.identity_data['token_uri'], data=values)
        with open(ACCESS_TOKEN_FILE, 'w') as f:
            json.dump(response.json(), f)
        self.access_token = response.json()['access_token']
        logging.info('Token has been refreshed.')
