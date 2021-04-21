import json
import os
import webbrowser
import requests
from config import *
from exceptions import *

SCOPES = [
    'https://www.googleapis.com/auth/photoslibrary.readonly',
    # 'https://www.googleapis.com/auth/photoslibrary',
    # 'https://www.googleapis.com/auth/photoslibrary.edit.appcreateddata',
    # 'https://www.googleapis.com/auth/photoslibrary.sharing',
          ]


class Authentication:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.identity_data = None
        self.access_token = None
        self.refresh_token = None

    @property
    def url(self):
        scopes_for_uri = ''.join(i + '%20' for i in SCOPES)
        return f"{self.identity_data['auth_uri']}?scope={scopes_for_uri}&response_type=code&" \
               f"redirect_uri={self.identity_data['redirect_uris'][0]}&client_id={self.identity_data['client_id']}"

    def __read_identity_data(self):
        try:
            with open(IDENTITY_FILE_PATH) as file_data:
                self.identity_data = json.load(file_data)['installed']
        except OSError as err:
            self.logger.error(f'Error while reading {IDENTITY_FILE_PATH}.\n{err}')
            raise
        except KeyError:
            self.logger.error(f"Invalid {IDENTITY_FILE_PATH} file.")
            raise

    def __is_authenticated(self) -> bool:
        self.__read_identity_data()
        try:
            self.__get_tokens()
        except FileNotFoundError:
            return False
        return True

    def authenticate(self):
        if self.__is_authenticated():
            return True
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
            raise Exception(f"Not authenticated. http code: {response.status_code}, response: {response.text}.")
        try:
            with open(OAUTH2_FILE_PATH, 'w') as file:
                json.dump(response.json(), file)
        except OSError as err:
            self.logger.error(f'Error while writing {OAUTH2_FILE_PATH}.\n{err}')
            raise
        self.access_token = response.json()['access_token']
        self.refresh_token = response.json()['refresh_token']
        self.logger.warning("Authenticated successfully.")

    def __get_tokens(self):
        try:
            with open(OAUTH2_FILE_PATH) as file:
                oauth_file_data = json.load(file)
                self.refresh_token = oauth_file_data['refresh_token']
                self.access_token = oauth_file_data['access_token']
        except FileNotFoundError:
            self.logger.warning("Authentication require.")
            raise
        except OSError as err:
            self.logger.error(f"Fail to read {OAUTH2_FILE_PATH}, {err}")
            raise
        except KeyError as err:
            self.logger.error(f"File does not contain tokens, {err}")
            raise
        if os.path.exists(ACCESS_TOKEN_FILE):
            with open(ACCESS_TOKEN_FILE) as file:
                file_content = file.read()
                try:
                    self.access_token = json.loads(file_content)['access_token']
                except KeyError:
                    self.logger.error(f"File {ACCESS_TOKEN_FILE} exists but does not contain the access token, "
                                      f"{file_content}")
                    raise

    def refresh_access_token(self):
        values = {'client_id': self.identity_data['client_id'],
                  'client_secret': self.identity_data['client_secret'],
                  'refresh_token': self.refresh_token,
                  'grant_type': 'refresh_token'}
        response = requests.post(self.identity_data['token_uri'], data=values)
        try:
            with open(ACCESS_TOKEN_FILE, 'w') as f:
                json.dump(response.json(), f)
        except OSError as err:
            self.logger.error(f"Fail to write the access token to {ACCESS_TOKEN_FILE} file, {err}")
            raise
        self.access_token = response.json()['access_token']
        self.logger.info('Token has been refreshed.')
