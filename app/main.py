import shutil
import logging
import os
import sqlite3

from datetime import datetime, timedelta
from app.tools import media, exceptions, helpers
from app.config import config
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from time import sleep

SCOPES = [
    'https://www.googleapis.com/auth/photoslibrary.readonly',
    # 'https://www.googleapis.com/auth/photoslibrary',
    # 'https://www.googleapis.com/auth/photoslibrary.edit.appcreateddata',
    # 'https://www.googleapis.com/auth/photoslibrary.sharing',
]


class Authentication:
    def __init__(self):
        self.creds = None
        self.__logger = logging.getLogger(self.__class__.__name__)

    @property
    def access_token(self) -> str:
        if not self.creds:
            if os.path.exists(config.ACCESS_TOKEN_FILE_PATH):
                self.creds = Credentials.from_authorized_user_file(config.ACCESS_TOKEN_FILE_PATH, SCOPES)
            else:
                self.__get_auth()
        if self.creds.expired:
            self.creds.refresh(Request())
            self.__write_access_token_to_file()
            self.__logger.info('Access token refreshed.')
        if not self.creds.valid:
            raise exceptions.AuthUnsuccessful()
        return self.creds.token

    def __write_access_token_to_file(self):
        with open(config.ACCESS_TOKEN_FILE_PATH, 'w') as token:
            token.write(self.creds.to_json())

    def __get_auth(self):
        flow = InstalledAppFlow.from_client_secrets_file(
            config.IDENTITY_FILE_PATH, SCOPES)
        self.creds = flow.run_local_server(port=0)
        self.__logger.info('Successfully authenticated.')
        self.__write_access_token_to_file()


class MetadataList:
    def __init__(self, db_conn):
        self.__new_next_page_token = None
        self.__current_mode = '0'
        self.__list_retrieved = False
        self.__page = None
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__db_conn = db_conn

    def __get_page(self, auth, next_page_token):
        url = media.SRV_ENDPOINT + 'mediaItems'
        objects_count_on_page = '100'
        params = {'pageSize': objects_count_on_page,
                  'pageToken': next_page_token}
        representation = helpers.make_request_w_auth(auth.access_token, url, params)
        try:
            self.__page = representation['mediaItems']
        except KeyError:
            raise exceptions.NoItemsInResp(f"No mediaItems object in response. Response: {representation}")
        try:
            self.__new_next_page_token = representation['nextPageToken']
        except KeyError:
            self.__new_next_page_token = None
            raise exceptions.NoNextPageTokenInResp("No nextPageToken object in response. Probably got end of the list.")

    def __write_page(self, mode='write_all'):
        """
        :param mode: 'write_all' or 'write_latest'
        """
        for item in self.__page:
            media_item = media.Item(item['id'], item['mimeType'], item['filename'],
                                    item['mediaMetadata']['creationTime'], self.__db_conn,
                                    config.PATH_TO_VIDEOS_STORAGE, config.PATH_TO_IMAGES_STORAGE)
            try:
                media_item.write_to_db()
            except exceptions.ObjAlreadyExists:
                if mode == 'write_all':
                    continue
                elif mode == 'write_latest':
                    raise
                else:
                    raise exceptions.MyGPhotoException('Unexpected error.')

    def __check_mode(self):
        cursor = self.__db_conn.cursor()
        try:
            cursor.execute("SELECT value FROM account_info WHERE key='list_received'")
        except sqlite3.Error as err:
            self.__logger.error(f'DB query failed, {err}.')
            raise
        try:
            self.__current_mode = cursor.fetchone()[0]
        except TypeError:
            pass

    @staticmethod
    def get_items_by_ids(ids: tuple, auth) -> list:
        url = media.SRV_ENDPOINT + 'mediaItems:batchGet?' + ''.join('mediaItemIds=' + i + '&' for i in ids)
        representation = helpers.make_request_w_auth(auth.access_token, url)
        try:
            items = representation['mediaItemResults']
        except KeyError:
            raise exceptions.NoItemsInResp()
        return items

    def get_metadata_list(self, auth):
        """Gets media metadata from Google Photo server and writes it to the local database."""
        self.__check_mode()
        cursor = self.__db_conn.cursor()
        pages = 0
        while True:
            try:
                self.__get_page(auth, self.__new_next_page_token)
            except (exceptions.NoItemsInResp, exceptions.NoNextPageTokenInResp):
                self.__list_retrieved = True
            except exceptions.FailGettingPage:
                break
            if self.__current_mode == '0':
                self.__write_page()
            elif self.__current_mode == '1':
                try:
                    self.__write_page(mode='write_latest')
                except exceptions.ObjAlreadyExists:
                    break
            else:
                raise Exception('Unexpected error.')
            pages += 1
            self.__logger.info(f'{pages} - processed.')
            if self.__list_retrieved:
                cursor.execute("INSERT OR REPLACE INTO account_info (key, value) VALUES ('list_received', '1') ")
                self.__db_conn.commit()
                self.__logger.warning('List of media has been retrieved.')
                break


class LocalStorage:
    def __init__(self, db_conn):
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__db_conn = db_conn
        self.__download_selection = None
        self.__actualization_selection = None
        self.__last_actualization_date = None

    def __get_download_selection(self):
        try:
            cursor = self.__db_conn.cursor()
            cursor.execute("SELECT object_id, media_type, filename, creation_time FROM my_media "
                           "WHERE stored = '0' ORDER BY creation_time DESC")
        except sqlite3.Error as err:
            self.__logger.error(f'Fail to communicate with DB.\n{err}')
            raise
        self.__download_selection = cursor.fetchall()

    def __create_tree(self):
        sub_folders = set()
        for item in self.__download_selection:
            year = datetime.strptime(item[3], "%Y-%m-%dT%H:%M:%SZ").year
            sub_folders.add(str(year))
        for item in sub_folders:
            if not os.path.exists(config.PATH_TO_IMAGES_STORAGE + item):
                try:
                    os.makedirs(config.PATH_TO_IMAGES_STORAGE + item)
                except OSError as err:
                    self.__logger.error(f"Fail to create folder, {err}")
                    raise
                self.__logger.info(f"Folder {config.PATH_TO_IMAGES_STORAGE + item} has been created.")
            if not os.path.exists(config.PATH_TO_VIDEOS_STORAGE + item):
                try:
                    os.makedirs(config.PATH_TO_VIDEOS_STORAGE + item)
                except OSError as err:
                    self.__logger.error(f"Fail to create folder, {err}")
                    raise
                self.__logger.info(f"Folder {config.PATH_TO_VIDEOS_STORAGE + item} has been created.")

    def __get_actualization_selection(self, limit=5, start_from=None):  # TODO
        cursor = self.__db_conn.cursor()
        cursor.execute("SELECT value FROM account_info WHERE key = 'last_processed_object_id'")
        last_local_id_processed = cursor.fetchall()
        cursor.execute("DELETE from account_info WHERE key = 'last_processed_object_id'")
        self.__db_conn.commit()
        not_before = datetime.now() - timedelta(days=config.ACTUALIZATION_NOT_OLD)
        if not last_local_id_processed:
            cursor.execute("SELECT object_id, media_type, filename, creation_time FROM my_media WHERE stored != '0' \
                           and creation_time > ? ORDER BY id;", (not_before.strftime("%Y-%m-%d"),))
        else:
            cursor.execute("SELECT object_id, media_type, filename, creation_time FROM my_media WHERE stored != '0' and\
                           id > (SELECT id FROM my_media WHERE object_id = ?) and creation_time > ? ORDER BY id",
                           (last_local_id_processed[0][0], not_before.strftime("%Y-%m-%d")))
        self.__actualization_selection = cursor.fetchall()

    def __get_last_actualization(self):
        cursor = self.__db_conn.cursor()
        cursor.execute("SELECT value FROM account_info WHERE key = 'last_actualization'")
        try:
            self.__last_actualization_date = cursor.fetchone()[0]
        except TypeError:
            pass

    def is_actualization_needed(self) -> bool:
        self.__get_last_actualization()
        if not self.__last_actualization_date:
            return True
        difference = datetime.now() - \
            datetime.strptime(self.__last_actualization_date, "%Y-%m-%dT%H:%M:%SZ")
        difference = difference.days
        if difference < config.ACTUALIZATION_RUN_PERIOD:
            return False
        return True

    def __set_last_actualization_date(self):
        cursor = self.__db_conn.cursor()
        now = datetime.today().strftime("%Y-%m-%dT%H:%M:%SZ")
        cursor.execute(f"INSERT OR REPLACE INTO account_info (key, value) VALUES ('last_actualization', '{now}')")
        self.__db_conn.commit()

    def __write_last_processed(self, item_id):
        cursor = self.__db_conn.cursor()
        cursor.execute("INSERT INTO account_info (key, value) "
                       "VALUES('last_processed_object_id', ?)", (item_id,))
        self.__db_conn.commit()

    def remove_not_existing(self, auth) -> bool:  # TODO: use batchGet
        self.__get_actualization_selection()
        for item in self.__actualization_selection:
            media_item = media.Item(*item, self.__db_conn, config.PATH_TO_VIDEOS_STORAGE, config.PATH_TO_IMAGES_STORAGE)
            try:
                result = media_item.is_exist_on_server(auth)
            except (Exception, KeyboardInterrupt):
                self.__write_last_processed(media_item.id)
                raise
            if not result:
                media_item.remove_from_local()
                media_item.remove_from_db()
                self.__logger.info(f"{media_item.filename} removed from local and db.")
        self.__set_last_actualization_date()
        return True

    def download_media_items(self, auth):
        """Downloads media items that listed in the database putting it by year's folder."""
        self.__get_download_selection()
        try:
            self.__create_tree()
        except OSError:
            self.__logger.error("Please check storage paths in config.")
            raise
        for item in self.__download_selection:
            media_item = media.Item(*item, self.__db_conn,
                                    config.PATH_TO_VIDEOS_STORAGE, config.PATH_TO_IMAGES_STORAGE)
            try:
                media_item.get_base_url(auth)
            except FileNotFoundError:
                self.__logger.warning(f"Item {item[2]} not found on the server, removing from database.")
                media_item.remove_from_db()
                continue
            except exceptions.VideoNotReady:
                continue
            try:
                media_item.download()
            except (FileExistsError, OSError):
                continue
            except exceptions.DownloadError:
                sleep(30)
        self.__logger.info('Getting media items is complete.')


class Main:
    def __init__(self):
        logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(funcName)s: %(message)s',
                            filename=config.LOG_FILE_PATH,
                            filemode='a', level=logging.INFO)
        self.logger = logging.getLogger(self.__class__.__name__)
        if not self.__is_db_exists():
            self.__db_creation()
        self.db_conn = helpers.db_connect(config.DB_FILE_PATH)
        self.authentication = Authentication()
        self.metadata = MetadataList(self.db_conn)
        self.local_storage = LocalStorage(self.db_conn)

    def __is_db_exists(self) -> bool:
        if not os.path.exists(config.DB_FILE_PATH):
            message = f'DB {config.DB_FILE_PATH} does not exist.'
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
            shutil.copy('db/db.sqlite.structure', config.DB_FILE_PATH)
        except OSError as err:
            message = f'Fail to create DB.\n{err}'
            print(message)
            self.logger.error(message)
            exit(4)

    def main(self):
        self.logger.info('Starting...')
        try:
            self.metadata.get_metadata_list(self.authentication)
            self.logger.info('Start downloading a list of media items.')
            self.local_storage.download_media_items(self.authentication)
            # For testing maybe=========>
            # self.metadata.get_items_by_ids(
            #    ('AOrhRz6_TXeXp9ZUH278oCwL0g7Df7UaEwjHdrpJ8hD2eJg7nB3TK6Sf5Wj011eG42c4c_xKlvDZOqAsZDHHd4QPGa1cgEsc4A',
            #     'AOrhRz4w4HBXKfBvDswN9WDtbM-TwcieQj1l74Aj5tCK9sEN23QVoLiL_6iVOd7ARn2c5CW1S76yVLsjSd1sRtn7kpimctfr4w',
            #     'AOrhRz7jJd4KI_XKFxm0-GiSh7CbarOElRjKOo2q_aFL2k5bjgnPxG8B6g1RNHRg7aPl0T3Awx9a-s7-1w6Nh9IeZE4Bikt0Zw'),
            #    self.authentication
            # )
            # <================
            if self.local_storage.is_actualization_needed():
                self.logger.info("Start local DB and storage actualization.")
                self.local_storage.remove_not_existing(self.authentication)
                self.logger.info('Actualization is complete.')
        except KeyboardInterrupt:
            self.logger.warning("Aborted by user.")
            raise
        except Exception as err:
            self.logger.exception(f'Something went wrong.\n{err}')
        finally:
            self.db_conn.close()
        self.logger.info('Finished.')
