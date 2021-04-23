#!/usr/bin/env python3.8
# This is an application that gets, downloads media files and metadata from your Google Photo storage to your
# local storage.

import json
import webbrowser
import shutil
import time
from lib import *

SCOPES = [
    'https://www.googleapis.com/auth/photoslibrary.readonly',
    # 'https://www.googleapis.com/auth/photoslibrary',
    # 'https://www.googleapis.com/auth/photoslibrary.edit.appcreateddata',
    # 'https://www.googleapis.com/auth/photoslibrary.sharing',
          ]


class Authentication:
    def __init__(self, db_conn):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.identity_data = None
        self.__access_token = None
        self.__refresh_token = None
        self.__refresh_token_time = None
        self.__db_conn = db_conn

    @property
    def __url(self):
        scopes_for_uri = ''.join(i + '%20' for i in SCOPES)
        return f"{self.identity_data['auth_uri']}?scope={scopes_for_uri}&response_type=code&" \
               f"redirect_uri={self.identity_data['redirect_uris'][0]}&client_id={self.identity_data['client_id']}"

    @property
    def access_token(self):
        now = time.time()
        difference = int(now) - int(self.__refresh_token_time)
        if difference > 3599:
            self.__refresh_access_token()
        return self.__access_token

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
        try:
            self.__get_tokens()
        except FileNotFoundError:
            return False
        return True

    def __get_token_time(self):
        cursor = self.__db_conn.cursor()
        cursor.execute("SELECT value FROM account_info WHERE key='token_refreshed'")
        try:
            self.__refresh_token_time = cursor.fetchone()[0]
        except TypeError:
            return False
        return True

    def __get_tokens(self):
        try:
            with open(OAUTH2_FILE_PATH) as file:
                oauth_file_data = json.load(file)
                self.__refresh_token = oauth_file_data['refresh_token']
                self.__access_token = oauth_file_data['access_token']
        except FileNotFoundError:
            self.logger.warning('Authentication require.')
            raise
        except OSError as err:
            self.logger.error(f'Fail to read {OAUTH2_FILE_PATH}.\n{err}')
            raise
        except KeyError as err:
            self.logger.error(f'File does not contain tokens.\n{err}')
            raise

    def __read_latest_token(self):
        if not os.path.exists(ACCESS_TOKEN_FILE):
            return True
        with open(ACCESS_TOKEN_FILE) as file:
            file_content = file.read()
            try:
                self.__access_token = json.loads(file_content)['access_token']
            except KeyError:
                self.logger.error(f'File {ACCESS_TOKEN_FILE} exists but does not contain the access token, '
                                  f'{file_content}')
                raise

    def __set_token_time_now(self):
        cursor = self.__db_conn.cursor()
        now = int(time.time())
        self.__refresh_token_time = str(now)
        cursor.execute("INSERT OR REPLACE INTO `account_info` (key, value) VALUES ('token_refreshed', ?)", (str(now),))
        self.__db_conn.commit()

    def __refresh_access_token(self):
        values = {'client_id': self.identity_data['client_id'],
                  'client_secret': self.identity_data['client_secret'],
                  'refresh_token': self.__refresh_token,
                  'grant_type': 'refresh_token'}
        response = requests.post(self.identity_data['token_uri'], data=values)
        try:
            with open(ACCESS_TOKEN_FILE, 'w') as f:
                json.dump(response.json(), f)
        except OSError as err:
            self.logger.error(f"Fail to write the access token to {ACCESS_TOKEN_FILE} file, {err}")
            raise
        self.__access_token = response.json()['access_token']
        self.__set_token_time_now()
        self.logger.info('Token has been refreshed.')

    def authenticate(self):
        self.__read_identity_data()
        if self.__is_authenticated():
            self.__read_latest_token()
            if not self.__get_token_time():
                self.__refresh_access_token()
            return True
        print(f"If you do not have local browser please visit url: {self.__url}")
        webbrowser.open(self.__url, new=0, autoraise=True)
        code = input("Please enter the code: ")
        data = {'code': code,
                'client_id': self.identity_data['client_id'],
                'client_secret': self.identity_data['client_secret'],
                'redirect_uri': self.identity_data['redirect_uris'][0],
                'grant_type': 'authorization_code'}
        response = requests.post(self.identity_data['token_uri'], data=data)
        if response.status_code != 200:
            raise SessionNotAuth(f"http code: {response.status_code}, response: {response.text}.")
        try:
            with open(OAUTH2_FILE_PATH, 'w') as file:
                json.dump(response.json(), file)
        except OSError as err:
            self.logger.error(f'Error while writing {OAUTH2_FILE_PATH}.\n{err}')
            raise
        self.__access_token = response.json()['access_token']
        self.__refresh_token = response.json()['refresh_token']
        self.__set_token_time_now()
        self.logger.warning("Authenticated successfully.")


class MetadataList:
    def __init__(self, db_conn):
        self.new_next_page_token = None
        self.current_mode = '0'
        self.list_retrieved = False
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__db_conn = db_conn

    def __get_page(self, auth, next_page_token) -> json:
        url = SRV_ENDPOINT + 'mediaItems'
        objects_count_on_page = '100'
        params = {'pageSize': objects_count_on_page,
                  'pageToken': next_page_token}
        headers = {'Accept': 'application/json',
                   'Authorization': 'Bearer ' + auth.access_token}
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 401:
            raise SessionNotAuth("Unauthorized.")
        elif response.status_code != 200:
            raise FailGettingPage(f"http code {response.status_code} when trying to get page of list with "
                                  f"next_page_token: {next_page_token}, response: {response.text}")
        try:
            page = response.json()['mediaItems']
        except KeyError:
            raise NoItemsInResp(f"No mediaItems object in response. Response: {response.text}")
        try:
            self.new_next_page_token = response.json()['nextPageToken']
        except KeyError:
            self.new_next_page_token = None
            raise NoNextPageTokenInResp("No nextPageToken object in response. Probably got end of the list.")
        return page

    @staticmethod
    def __write_page(page, mode='write_all'):
        """
        :param mode: 'write_all' or 'write_latest'
        """
        for item in page:
            media_item = MediaItem(item['id'], item['mimeType'], item['filename'],
                                   item['mediaMetadata']['creationTime'])
            try:
                media_item.write_to_db()
            except ObjAlreadyExists:
                if mode == 'write_all':
                    continue
                elif mode == 'write_latest':
                    raise
                else:
                    raise Exception('Unexpected error.')

    def __check_mode(self):
        cursor = self.__db_conn.cursor()
        try:
            cursor.execute("SELECT value FROM account_info WHERE key='list_received'")
        except sqlite3.Error as err:
            self.logger.error(f'DB query failed, {err}.')
            raise
        try:
            self.current_mode = cursor.fetchone()[0]
        except TypeError:
            pass

    def get_metadata_list(self, auth):
        """Gets media metadata from Google Photo server and writes it to the local database."""
        self.__check_mode()
        cursor = self.__db_conn.cursor()
        self.logger.info(f'Running in mode {self.current_mode}.')
        pages = 0
        while True:
            try:
                page = self.__get_page(auth, self.new_next_page_token)
            except (NoNextPageTokenInResp, NoItemsInResp):
                self.list_retrieved = True
            except FailGettingPage:
                break
            if self.current_mode == '0':
                self.__write_page(page)
            elif self.current_mode == '1':
                try:
                    self.__write_page(page, mode='write_latest')
                except ObjAlreadyExists:
                    break
            else:
                raise Exception('Unexpected error.')
            pages += 1
            self.logger.info(f'{pages} - processed.')
            if self.list_retrieved:
                cursor.execute("UPDATE account_info SET value='1' WHERE key='list_received'")
                self.__db_conn.commit()
                self.logger.warning("List has been retrieved.")
                break


class LocalStorage:
    def __init__(self, db_conn):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__db_conn = db_conn
        self.download_selection = None
        self.actualization_selection = None
        self.last_actualization_date = None

    def __get_download_selection(self):
        try:
            cursor = self.__db_conn.cursor()
            cursor.execute("SELECT object_id, media_type, filename, creation_time FROM my_media "
                           "WHERE stored = '0' ORDER BY creation_time DESC")
        except sqlite3.Error as err:
            self.logger.error(f'Fail to communicate with DB.\n{err}')
            raise
        self.download_selection = cursor.fetchall()

    def __create_tree(self):
        sub_folders = set()
        for item in self.download_selection:
            year = datetime.datetime.strptime(item[3], "%Y-%m-%dT%H:%M:%SZ").year
            sub_folders.add(str(year))
        for item in sub_folders:
            if not os.path.exists(PATH_TO_IMAGES_STORAGE + item):
                try:
                    os.makedirs(PATH_TO_IMAGES_STORAGE + item)
                except OSError as err:
                    self.logger.error(f"Fail to create folder, {err}")
                    raise
                self.logger.info(f"Folder {PATH_TO_IMAGES_STORAGE + item} has been created.")
            if not os.path.exists(PATH_TO_VIDEOS_STORAGE + item):
                try:
                    os.makedirs(PATH_TO_VIDEOS_STORAGE + item)
                except OSError as err:
                    self.logger.error(f"Fail to create folder, {err}")
                    raise
                self.logger.info(f"Folder {PATH_TO_VIDEOS_STORAGE + item} has been created.")

    def __get_actualization_selection(self):
        cursor = self.__db_conn.cursor()
        cursor.execute("SELECT value FROM account_info WHERE key = 'last_processed_object_id'")
        last_local_id_processed = cursor.fetchall()
        if not last_local_id_processed:
            cursor.execute("SELECT object_id, media_type, filename, creation_time FROM my_media "
                           "WHERE stored != '0' ORDER BY id")
        else:
            cursor.execute("SELECT object_id, media_type, filename, creation_time FROM my_media WHERE \
                                   stored != '0' and id > (SELECT id FROM my_media WHERE object_id = ?) ORDER BY id",
                           last_local_id_processed[0])
        self.actualization_selection = cursor.fetchall()

    def __get_last_actualization(self):
        cursor = self.__db_conn.cursor()
        cursor.execute("DELETE from account_info WHERE key = 'last_processed_object_id'")
        self.__db_conn.commit()
        cursor.execute("SELECT value FROM account_info WHERE key = 'last_actualization'")
        try:
            self.last_actualization_date = cursor.fetchone()[0]
        except TypeError:
            pass

    def __is_actualization_needed(self) -> bool:
        if not self.last_actualization_date:
            return True
        difference = datetime.datetime.now() - \
            datetime.datetime.strptime(self.last_actualization_date, "%Y-%m-%dT%H:%M:%SZ")
        difference = difference.days
        if difference < ACTUALIZATION_PERIOD:
            return False
        return True

    def __set_last_actualization_date(self):
        cursor = self.__db_conn.cursor()
        now = datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%SZ")
        cursor.execute(f"INSERT OR REPLACE INTO account_info (key, value) VALUES ('last_actualization', '{now}')")
        self.__db_conn.commit()

    def __write_last_processed(self, item_id):
        cursor = self.__db_conn.cursor()
        cursor.execute("INSERT INTO account_info (key, value) "
                       "VALUES('last_processed_object_id', ?)", (item_id,))
        self.__db_conn.commit()

    def find_and_clean_not_existing(self, auth) -> bool:  # TODO: use batchGet
        if not self.__is_actualization_needed():
            return False
        self.logger.info("Start local DB and storage actualization.")
        self.__get_last_actualization()
        self.__get_actualization_selection()
        for item in self.actualization_selection:
            media_item = MediaItem(*item)
            try:
                result = media_item.is_exist_on_server(auth)
            except (Exception, KeyboardInterrupt):
                self.__write_last_processed(media_item.id)
                raise
            if not result:
                media_item.remove_from_local()
                media_item.remove_from_db()
                self.logger.info(f"{media_item.filename} removed from local and db.")
        self.__set_last_actualization_date()
        return True

    def get_media_items(self, auth):
        """Downloads media items that listed in the database putting it by years folder."""
        self.__get_download_selection()
        try:
            self.__create_tree()
        except OSError:
            self.logger.error("Please check storage paths in config.")
            raise
        for item in self.download_selection:
            media_item = MediaItem(*item)
            try:
                media_item.get_base_url(auth)
            except FileNotFoundError:
                self.logger.warning(f"Item {item[2]} not found on the server, removing from database.")
                media_item.remove_from_db()
                continue
            except VideoNotReady:
                continue
            try:
                media_item.download()
            except (FileExistsError, OSError):
                continue
        self.logger.info('Getting media items is complete.')


class Runtime:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        if not self.__is_db_exists():
            self.__db_creation()
        self.db_conn = db_connect()
        self.authentication = Authentication(self.db_conn)
        self.metadata = MetadataList(self.db_conn)
        self.local_storage = LocalStorage(self.db_conn)

    def __is_db_exists(self) -> bool:
        if not os.path.exists(DB_FILE_PATH):
            message = f'DB {DB_FILE_PATH} does not exist.'
            print(message)
            self.logger.error(message)
            return False
        return True

    def __db_creation(self):
        answer = input('Do you want to create new DB?(Y/n)')
        if answer == 'n' or answer == 'N':
            self.logger.warning('Aborted by user.')
            exit(3)
        try:
            shutil.copy(DB_FILE_PATH + '.structure', DB_FILE_PATH)
        except OSError as err:
            message = f'Fail to create DB.\n{err}'
            print(message)
            self.logger.error(message)
            exit(4)

    def main(self):
        self.logger.info('Started.')
        self.authentication.authenticate()
        try:
            self.metadata.get_metadata_list(self.authentication)
        except KeyboardInterrupt:
            self.logger.warning("Aborted by user.")
            exit(3)
        self.logger.info('Start downloading a list of media items.')
        try:
            self.local_storage.get_media_items(self.authentication)
        except KeyboardInterrupt:
            self.logger.warning("Aborted by user.")
            exit(3)
        try:
            self.local_storage.find_and_clean_not_existing(self.authentication)
        except KeyboardInterrupt:
            self.logger.warning("Aborted by user.")
            exit(3)
        self.logger.info('Actualization is complete.')
        self.db_conn.close()
        self.logger.info('Finished.')


if __name__ == '__main__':
    Runtime().main()
