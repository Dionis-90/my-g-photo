#!/usr/bin/env python3.7

# This is an application that gets, downloads media files and metadata from your Google Photo storage to your
# local storage.

import sqlite3
import datetime
from authentication import *
from exceptions import *

# Define constants
SRV_ENDPOINT = 'https://photoslibrary.googleapis.com/v1/'
DB = sqlite3.connect(DB_FILE_PATH)
DB_CONNECTION = DB.cursor()


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

    def write_metadata_to_db(self):
        values = (self.id, self.filename, self.mime_type, self.creation_time)
        try:
            DB_CONNECTION.execute('INSERT INTO my_media (object_id, filename, media_type, creation_time) \
                    VALUES (?, ?, ?, ?)', values)
            DB.commit()
        except sqlite3.IntegrityError:
            raise ObjAlreadyExists(f"Media item {self.filename} already in the DB.")
        except sqlite3.Error as err:
            self.logger.error(f'Fail to write {self.filename} metadata into the DB. Error {err}')

    def remove_metadata_from_db(self):
        try:
            DB_CONNECTION.execute("DELETE FROM my_media WHERE object_id=?", (self.id,))
            DB.commit()
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
            response = requests.get(self.base_url + '=d', params=None, headers=None)
            if 'text/html' in response.headers['Content-Type']:
                raise DownloadError(f"Fail to download {self.filename}. Server returns: {response.text}")
            elif 'image' in response.headers['Content-Type']:
                if os.path.exists(PATH_TO_IMAGES_STORAGE + self.sub_folder_name + self.filename):
                    self.logger.warning(f"File {self.filename} already exist in local storage! Setting 'stored = 2' "
                                        f"in database.")
                    DB_CONNECTION.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (self.id,))
                    DB.commit()
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
            DB_CONNECTION.execute("UPDATE my_media SET stored='1' WHERE object_id=?", (self.id,))
            DB.commit()

        def download_video_item():
            response = requests.get(self.base_url + '=dv', params=None, headers=None, stream=True)
            if 'text/html' in response.headers['Content-Type']:
                raise DownloadError(f"Fail to download {self.filename}. Server returns: {response.text}")
            if 'video' in response.headers['Content-Type']:
                if os.path.exists(PATH_TO_VIDEOS_STORAGE + self.sub_folder_name + self.filename):
                    self.logger.warning(f"File {self.filename} already exist in local storage! Setting 'stored = 2' "
                                        f"in database.")
                    DB_CONNECTION.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (self.id,))
                    DB.commit()
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
            DB_CONNECTION.execute("UPDATE my_media SET stored='1' WHERE object_id=?", (self.id,))
            DB.commit()

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


class MetadataHandler:
    """Gets pages with media metadata from Google Photo server and writes it to the database."""
    def __init__(self):
        self.list_one_page = []
        self.new_next_page_token = None
        self.current_mode = '0'
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_page(self, auth, next_page_token):
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
            self.list_one_page = response.json()['mediaItems']
        except KeyError:
            raise NoItemsInResp(f"No mediaItems object in response. Response: {response.text}")
        try:
            self.new_next_page_token = response.json()['nextPageToken']
        except KeyError:
            self.new_next_page_token = None
            raise NoNextPageTokenInResp("No nextPageToken object in response. Probably got end of the list.")

    def write_page(self, mode='write_all'):
        """
        :param mode: 'write_all' or 'write_latest'
        """
        for item in self.list_one_page:
            media_item = MediaItem(item['id'], item['mimeType'], item['filename'],
                                   item['mediaMetadata']['creationTime'])
            try:
                media_item.write_metadata_to_db()
            except ObjAlreadyExists:
                if mode == 'write_all':
                    continue
                elif mode == 'write_latest':
                    raise
                else:
                    raise Exception('Unexpected error.')

    def check_mode(self):
        try:
            DB_CONNECTION.execute("SELECT value FROM account_info WHERE key='list_received'")
        except sqlite3.Error as err:
            self.logger.error(f'DB query failed, {err}.')
            raise
        try:
            self.current_mode = DB_CONNECTION.fetchone()[0]
        except TypeError:
            pass


class LocalStoreHandler:
    """Downloads media items that listed in the database."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        DB_CONNECTION.execute(
            "SELECT object_id, media_type, filename, creation_time FROM my_media\
             WHERE stored = '0' ORDER BY creation_time DESC")
        self.download_selection = DB_CONNECTION.fetchall()

    def create_tree(self):
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

    def get_media_items(self, auth):
        for item in self.download_selection:
            media_item = MediaItem(*item)
            try:
                media_item.get_base_url(auth)
            except FileNotFoundError:
                self.logger.warning(f"Item {item[2]} not found on the server, removing from database.")
                media_item.remove_metadata_from_db()
                continue
            except SessionNotAuth:
                auth.refresh_access_token()
                media_item.get_base_url(auth)
            except Exception:
                self.logger.error(f'Fail to update base url by {item[2]}.')
                raise
            try:
                media_item.download()
            except FileExistsError:
                continue
            except OSError:
                continue
        self.logger.info('Getting media items is complete.')


class Paginator:
    def __init__(self, auth):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth = auth
        self.listing = MetadataHandler()
        self.listing.check_mode()
        self.list_retrieved = False

    def get_whole_media_list(self):
        self.logger.info(f'Running in mode {self.listing.current_mode}.')
        page = 0
        while True:
            next_page_token = self.listing.new_next_page_token
            try:
                self.listing.get_page(self.auth, next_page_token)
            except SessionNotAuth:
                self.auth.refresh_access_token()
                continue
            except (NoNextPageTokenInResp, NoItemsInResp):
                self.list_retrieved = True
            except FailGettingPage:
                break
            if self.listing.current_mode == '0':
                self.listing.write_page()
            elif self.listing.current_mode == '1':
                try:
                    self.listing.write_page(mode='write_latest')
                except ObjAlreadyExists:
                    break
            else:
                raise Exception('Unexpected error.')
            page += 1
            self.logger.info(f'{page} - processed.')
            if self.list_retrieved:
                DB_CONNECTION.execute("UPDATE account_info SET value='1' WHERE key='list_received'")
                DB.commit()
                self.logger.warning("List has been retrieved.")
                break


class LocalStoreDBCleaner:
    def __init__(self, auth):
        self.logger = logging.getLogger(self.__class__.__name__)
        DB_CONNECTION.execute("SELECT value FROM account_info WHERE key = 'last_processed_object_id'")
        last_local_id_processed = DB_CONNECTION.fetchall()
        if not last_local_id_processed:
            DB_CONNECTION.execute(
                "SELECT object_id, media_type, filename, creation_time FROM my_media\
                WHERE stored != '0' ORDER BY id")
        else:
            DB_CONNECTION.execute("SELECT object_id, media_type, filename, creation_time FROM my_media WHERE \
                stored != '0' and id > (SELECT id FROM my_media WHERE object_id = ?) ORDER BY id",
                                  last_local_id_processed[0])
        self.local_metadata_selection = DB_CONNECTION.fetchall()
        DB_CONNECTION.execute("DELETE from account_info WHERE key = 'last_processed_object_id'")
        DB.commit()
        self.auth = auth
        DB_CONNECTION.execute("SELECT value FROM account_info WHERE key = 'last_actualization'")
        try:
            self.last_actualization_date = DB_CONNECTION.fetchone()[0]
        except TypeError:
            self.last_actualization_date = False

    def find_not_existing(self):
        for item in self.local_metadata_selection:
            media_item = MediaItem(*item)
            try:
                result = media_item.is_exist_on_server(self.auth)
            except SessionNotAuth:
                self.auth.refresh_access_token()
                try:
                    result = media_item.is_exist_on_server(self.auth)
                except (Exception, KeyboardInterrupt):
                    DB_CONNECTION.execute("INSERT INTO account_info (key, value) \
                        VALUES('last_processed_object_id', ?)", (media_item.id,))
                    DB.commit()
                    raise
            except (Exception, KeyboardInterrupt):
                DB_CONNECTION.execute("INSERT INTO account_info (key, value) \
                    VALUES('last_processed_object_id', ?)", (media_item.id,))
                DB.commit()
                raise
            if not result:
                media_item.remove_from_local()
                media_item.remove_metadata_from_db()
                self.logger.info(f"{media_item.filename} removed from local and db.")
        now = datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%SZ")
        DB_CONNECTION.execute(f"INSERT OR REPLACE INTO account_info (key, value) \
                                VALUES ('last_actualization', '{now}')")
        DB.commit()

    def is_actualization_needed(self) -> bool:
        if not self.last_actualization_date:
            return True
        difference = datetime.datetime.now() - \
            datetime.datetime.strptime(self.last_actualization_date, "%Y-%m-%dT%H:%M:%SZ")
        difference = difference.days
        if difference < ACTUALIZATION_PERIOD:
            return False
        else:
            return True


class Runtime:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info('Started.')
        self.authentication = Authentication()
        try:
            self.authentication.get_tokens()
        except FileNotFoundError:
            try:
                self.authentication.authenticate()
            except Exception as err:
                self.logger.error(f"Fail to authenticate, {err}")
                exit(5)
        except OSError as err:
            self.logger.error(f"Fail to authenticate, {err}")
            exit(6)
        except KeyError:
            self.logger.error("Fail to authenticate.")
            exit(4)

    def main(self):
        paginator = Paginator(self.authentication)
        try:
            paginator.get_whole_media_list()
        except Exception as err:
            self.logger.error(f"Unexpected error, {err}")
            DB.close()
            exit(1)
        self.logger.info('Start downloading a list of media items.')
        downloader = LocalStoreHandler()
        try:
            downloader.create_tree()
        except OSError:
            self.logger.error("Please check storage paths in config.")
            exit(2)
        try:
            downloader.get_media_items(self.authentication)
        except Exception as err:
            self.logger.error(f"Fail to download media: {err}")
            exit(3)
        except KeyboardInterrupt:
            self.logger.warning("Aborted by user.")
            exit(0)
        db_actualization = LocalStoreDBCleaner(self.authentication)
        result = db_actualization.is_actualization_needed()
        if result:
            self.logger.info("Start local DB and storage actualization.")
            try:
                db_actualization.find_not_existing()
            except Exception as err:
                self.logger.error(f"Fail to actualize DB, {err}")
                exit(8)
            except KeyboardInterrupt:
                self.logger.warning("Aborted by user.")
                exit(0)
            finally:
                DB.close()
            self.logger.info('Actualization is complete.')
        self.logger.info('Finished.')


if __name__ == '__main__':
    Runtime().main()
