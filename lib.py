import sqlite3
import datetime
from authentication import *
from exceptions import *


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
        self.creation_year: int = datetime.datetime.strptime(self.creation_time, "%Y-%m-%dT%H:%M:%SZ").year
        self.sub_folder_name = str(self.creation_year) + '/'
        self.logger = logging.getLogger(self.__class__.__name__)
        self.db_conn = db_connect()

    def write_to_db(self):
        cursor = self.db_conn.cursor()
        values = (self.id, self.filename, self.mime_type, self.creation_time)
        try:
            cursor.execute('INSERT INTO my_media (object_id, filename, media_type, creation_time) \
                    VALUES (?, ?, ?, ?)', values)
            self.db_conn.commit()
        except sqlite3.IntegrityError:
            raise ObjAlreadyExists(f"Media item {self.filename} already in the DB.")
        except sqlite3.Error as err:
            self.logger.error(f'Fail to write {self.filename} metadata into the DB. Error {err}')

    def remove_from_db(self):
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("DELETE FROM my_media WHERE object_id=?", (self.id,))
            self.db_conn.commit()
        except sqlite3.Error as err:
            self.logger.error(f'Fail to remove {self.filename} from the DB. Error {err}')

    def get_base_url(self, auth):  # TODO: Check if video object first
        headers = {'Accept': 'application/json',
                   'Authorization': 'Bearer ' + auth.access_token}
        response = requests.get(SRV_ENDPOINT + 'mediaItems/' + self.id, headers=headers)
        if response.status_code == 401:
            raise SessionNotAuth("Session unauthorized.")
        elif response.status_code == 404:
            self.logger.warning(f"Item {self.id} not found on the server.")
            raise FileNotFoundError()
        elif response.status_code != 200:
            raise Exception(f'Response code: {response.status_code}. Response: {response.text}')
        try:
            self.base_url = response.json()['baseUrl']
        except KeyError:
            self.logger.error(f'Response does not contain baseUrl. Response: {response.text}')
            raise

    def download(self):

        def download_photo_item():
            cursor = self.db_conn.cursor()
            response = requests.get(self.base_url + '=d', params=None, headers=None)
            if 'text/html' in response.headers['Content-Type']:
                raise DownloadError(f"Fail to download {self.filename}. Server returns: {response.text}")
            elif 'image' in response.headers['Content-Type']:
                if os.path.exists(PATH_TO_IMAGES_STORAGE + self.sub_folder_name + self.filename):
                    self.logger.warning(f"File {self.filename} already exist in local storage! Setting 'stored = 2' "
                                        f"in database.")
                    cursor.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (self.id,))
                    self.db_conn.commit()
                    raise FileExistsError()
                try:
                    with open(PATH_TO_IMAGES_STORAGE + self.sub_folder_name + self.filename, 'wb') as media_file:
                        media_file.write(response.content)
                except OSError as err:
                    self.logger.warning(f"Fail to download {self.filename} photo, {err}")
                    raise
            else:
                raise Exception('Unexpected content type.')
            self.logger.info(f"Media file {self.filename} stored.")
            cursor.execute("UPDATE my_media SET stored='1' WHERE object_id=?", (self.id,))
            self.db_conn.commit()

        def download_video_item():
            cursor = self.db_conn.cursor()
            response = requests.get(self.base_url + '=dv', params=None, headers=None, stream=True)
            if 'text/html' in response.headers['Content-Type']:
                raise DownloadError(f"Fail to download {self.filename}. Server returns: {response.text}")
            if 'video' in response.headers['Content-Type']:
                if os.path.exists(PATH_TO_VIDEOS_STORAGE + self.sub_folder_name + self.filename):
                    self.logger.warning(f"File {self.filename} already exist in local storage! Setting 'stored = 2' "
                                        f"in database.")
                    cursor.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (self.id,))
                    self.db_conn.commit()
                    raise FileExistsError()
                try:
                    with open(PATH_TO_VIDEOS_STORAGE + self.sub_folder_name + self.filename, 'wb') as media_file:
                        for chunk in response.iter_content(chunk_size=1024):
                            media_file.write(chunk)
                except OSError as err:
                    self.logger.warning(f"Fail to download {self.filename} video, {err}")
                    raise
            else:
                raise Exception('Unexpected content type.')
            self.logger.info(f"Media file {self.filename} stored.")
            cursor.execute("UPDATE my_media SET stored='1' WHERE object_id=?", (self.id,))
            self.db_conn.commit()

        if 'image' in self.mime_type:
            try:
                download_photo_item()
            except Exception:
                raise
        elif 'video' in self.mime_type:
            try:
                download_video_item()
            except Exception:
                raise
        else:
            raise Exception('Unexpected mime type.')

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
