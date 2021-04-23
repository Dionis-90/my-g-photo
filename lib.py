import sqlite3
import datetime
import requests
import os
from config import *
from exceptions import *

logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(funcName)s: %(message)s',
                    filename=LOG_FILE_PATH, filemode='a', level=logging.INFO)


def db_connect():
    db_logger = logging.getLogger('DB connection')
    try:
        db_conn = sqlite3.connect(DB_FILE_PATH)
    except Exception as err:
        message = f'Fail to connect to DB {DB_FILE_PATH}.\n{err}'
        print(message)
        db_logger.error(message)
        exit(10)
    return db_conn


SRV_ENDPOINT = 'https://photoslibrary.googleapis.com/v1/'


class MediaItem:
    def __init__(self, item_id, mime_type, filename, creation_time):
        self.id = item_id
        self.base_url = None
        self.mime_type = mime_type
        self.filename = filename
        self.creation_time = creation_time
        creation_year: int = datetime.datetime.strptime(self.creation_time, "%Y-%m-%dT%H:%M:%SZ").year
        self.sub_folder_name = str(creation_year) + '/'
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__db_conn = None
        self.video_status = None

    def write_to_db(self):
        self.__db_conn = db_connect()
        cursor = self.__db_conn.cursor()
        values = (self.id, self.filename, self.mime_type, self.creation_time)
        try:
            cursor.execute('INSERT INTO my_media (object_id, filename, media_type, creation_time) '
                           'VALUES (?, ?, ?, ?)', values)
            self.__db_conn.commit()
        except sqlite3.IntegrityError:
            raise ObjAlreadyExists(f'Media item {self.filename} already in the DB.')
        except sqlite3.Error as err:
            self.logger.error(f'Fail to write {self.filename} metadata into the DB.\n{err}')

    def remove_from_db(self):
        self.__db_conn = db_connect()
        cursor = self.__db_conn.cursor()
        try:
            cursor.execute("DELETE FROM my_media WHERE object_id=?", (self.id,))
            self.__db_conn.commit()
        except sqlite3.Error as err:
            self.logger.error(f'Fail to remove {self.filename} from the DB.\n{err}')

    def get_base_url(self, auth):
        headers = {'Accept': 'application/json',
                   'Authorization': 'Bearer ' + auth.access_token}
        response = requests.get(SRV_ENDPOINT + 'mediaItems/' + self.id, headers=headers)
        if response.status_code == 401:
            raise SessionNotAuth('Session unauthorized.')
        elif response.status_code == 404:
            self.logger.warning(f'Item {self.id} not found on the server.')
            raise FileNotFoundError()
        elif response.status_code != 200:
            raise Exception(f'Response code: {response.status_code}. Response: {response.text}')
        representation = response.json()
        try:
            if 'video' in self.mime_type:
                self.video_status = representation['mediaMetadata']['video']['status']
                if self.video_status != 'READY':
                    raise VideoNotReady(f'Video {self.filename} is not ready.')
        except KeyError:
            self.logger.error(f'Response does not contain video status. Response: {response.text}')
            raise
        try:
            self.base_url = representation['baseUrl']
        except KeyError:
            self.logger.error(f'Response does not contain baseUrl. Response: {response.text}')
            raise

    def download(self):
        self.__db_conn = db_connect()
        cursor = self.__db_conn.cursor()
        if 'image' in self.mime_type:
            url_suffix = '=d'
            path_to_object = PATH_TO_IMAGES_STORAGE + self.sub_folder_name + self.filename
        elif 'video' in self.mime_type:
            url_suffix = '=dv'
            path_to_object = PATH_TO_VIDEOS_STORAGE + self.sub_folder_name + self.filename
        else:
            raise Exception('Unexpected mime type.')
        response = requests.get(self.base_url + url_suffix, params=None, headers=None, stream=True)
        if 'text/html' in response.headers['Content-Type']:
            raise DownloadError(f"Fail to download {self.filename}. Server returns: {response.text}")
        elif 'image' in response.headers['Content-Type'] or 'video' in response.headers['Content-Type']:
            if os.path.exists(path_to_object):
                self.logger.warning(f"File {self.filename} already exist in local storage! Setting 'stored = 2' "
                                    f"in database.")
                cursor.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (self.id,))
                self.__db_conn.commit()
                raise FileExistsError()
            try:
                with open(path_to_object, 'wb') as media_file:
                    for chunk in response.iter_content(chunk_size=8192):
                        media_file.write(chunk)
            except OSError as err:
                self.logger.warning(f"Fail to download {self.filename}.\n{err}")
                raise
        else:
            raise Exception(f"Unexpected content type {response.headers['Content-Type']}")
        self.logger.info(f"Media file {self.filename} stored.")
        cursor.execute("UPDATE my_media SET stored='1' WHERE object_id=?", (self.id,))
        self.__db_conn.commit()

    def remove_from_local(self):
        if 'video' in self.mime_type:
            path_to_file = PATH_TO_VIDEOS_STORAGE + self.sub_folder_name + self.filename
        elif 'image' in self.mime_type:
            path_to_file = PATH_TO_IMAGES_STORAGE + self.sub_folder_name + self.filename
        else:
            raise Exception('Unexpected error.')
        try:
            os.remove(path_to_file)
        except OSError as err:
            self.logger.error(f"Fail to remove {self.filename}, {err}")

    def is_exist_on_server(self, auth) -> bool:
        headers = {'Accept': 'application/json',
                   'Authorization': 'Bearer ' + auth.access_token}
        response = requests.get(SRV_ENDPOINT + 'mediaItems/' + self.id, headers=headers)
        if response.status_code == 401:
            raise SessionNotAuth("Session unauthorized.")
        elif response.status_code == 404:
            self.logger.warning(f"Item {self.id} not found on the server.")
            return False
        elif response.status_code != 200:
            raise Exception(f'Response code: {response.status_code}. Response: {response.text}')
        return True
