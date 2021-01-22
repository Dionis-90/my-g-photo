#!/usr/bin/env python3.7

# This is an application that gets, downloads media files and metadata from your Google Photo storage to your
# local storage.

import sqlite3
import datetime
from authorization import *
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

    def write_metadata_to_db(self):
        values = (self.id, self.filename, self.mime_type, self.creation_time)
        try:
            DB_CONNECTION.execute('INSERT INTO my_media (object_id, filename, media_type, creation_time) \
                    VALUES (?, ?, ?, ?)', values)
            DB.commit()
        except sqlite3.IntegrityError:
            raise ObjAlreadyExists(f"Media item {self.filename} already in the DB.")
        except sqlite3.Error as err:
            logging.error(f'Fail to write {self.filename} metadata into the DB. Error {err}')

    def remove_metadata_from_db(self):
        try:
            DB_CONNECTION.execute("DELETE FROM my_media WHERE object_id=?", (self.id,))
            DB.commit()
        except sqlite3.Error as err:
            logging.error(f'Fail to remove {self.filename} from the DB. Error {err}')

    def update_base_url(self, auth):
        headers = {'Accept': 'application/json',
                   'Authorization': 'Bearer ' + auth.access_token}
        params = {'key': API_KEY}
        response = requests.get(SRV_ENDPOINT+'mediaItems/'+self.id, params=params, headers=headers)
        if response.status_code == 401:
            raise SessionNotAuth("Session unauthorized.")
        elif response.status_code == 404:
            logging.warning(f"Item {self.id} not found on the server.")
            raise FileNotFoundError()
        elif response.status_code != 200:
            logging.error(f'Response code: {response.status_code}. Response: {response.text}')
            raise Exception()
        try:
            self.base_url = response.json()['baseUrl']
        except KeyError:
            logging.error(f'Response does not contain baseUrl. Response: {response.text}')
            raise

    def download(self):
        sub_folder_name = str(self.creation_year)+'/'

        def download_photo_item():
            response = requests.get(self.base_url+'=d', params=None, headers=None)
            if 'text/html' in response.headers['Content-Type']:
                raise DownloadError(f"Fail to download {self.filename}. Server returns: {response.text}")
            elif 'image' in response.headers['Content-Type']:
                if os.path.exists(PATH_TO_IMAGES_STORAGE + sub_folder_name + self.filename):
                    logging.warning(f"File {self.filename} already exist in local storage! Setting 'stored = 2' "
                                    f"in database.")
                    DB_CONNECTION.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (self.id,))
                    DB.commit()
                    raise FileExistsError()
                media_file = open(PATH_TO_IMAGES_STORAGE + sub_folder_name + self.filename, 'wb')  # TODO:use try-except
                media_file.write(response.content)
                media_file.close()
            else:
                logging.error('Unexpected content type.')
                raise Exception()
            logging.info(f"Media file {self.filename} stored.")
            DB_CONNECTION.execute("UPDATE my_media SET stored='1' WHERE object_id=?", (self.id,))
            DB.commit()

        def download_video_item():
            response = requests.get(self.base_url+'=dv', params=None, headers=None, stream=True)
            if 'text/html' in response.headers['Content-Type']:
                raise DownloadError(f"Fail to download {self.filename}. Server returns: {response.text}")
            if 'video' in response.headers['Content-Type']:
                if os.path.exists(PATH_TO_VIDEOS_STORAGE + sub_folder_name + self.filename):
                    logging.warning(f"File {self.filename} already exist in local storage! Setting 'stored = 2' "
                                    f"in database.")
                    DB_CONNECTION.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (self.id,))
                    DB.commit()
                    raise FileExistsError()
                media_file = open(PATH_TO_VIDEOS_STORAGE + sub_folder_name + self.filename, 'wb')  # TODO:use try-except
                for chunk in response.iter_content(chunk_size=1024):
                    media_file.write(chunk)
                media_file.close()
            else:
                logging.error('Unexpected content type.')
                raise Exception()
            logging.info(f"Media file {self.filename} stored.")
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
            logging.error('Unexpected mime type.')
            raise Exception()


class Listing:
    """Gets pages with media metadata from Google Photo server and writes it to the database."""
    def __init__(self):
        self.list_one_page = []
        self.new_next_page_token = None
        self.current_mode = ''

    def get_page(self, auth, next_page_token):
        url = SRV_ENDPOINT + 'mediaItems'
        objects_count_on_page = '100'
        params = {'key': API_KEY,
                  'pageSize': objects_count_on_page,
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

    def write_metadata(self, mode='write_all'):
        """
        :param mode: 'write_all' or 'write_latest'
        """
        logging.info(f'Running in mode {mode}.')
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
                    logging.error('Unexpected error.')
                    raise Exception()

    def check_mode(self):
        try:
            DB_CONNECTION.execute("INSERT OR IGNORE INTO account_info (key, value) VALUES ('list_received', '0')")
            DB.commit()
            DB_CONNECTION.execute("SELECT value FROM account_info WHERE key='list_received'")
        except sqlite3.Error as err:
            logging.error(f'DB query failed, {err}.')
            raise
        self.current_mode = DB_CONNECTION.fetchone()[0]


class Downloader:
    """Downloads media items that listed in the database."""
    def __init__(self):
        DB_CONNECTION.execute(
            "SELECT object_id, media_type, filename, creation_time FROM my_media\
             WHERE stored = '0' ORDER BY creation_time DESC")
        self.selection = DB_CONNECTION.fetchall()

    def create_tree(self):
        sub_folders = set()
        for item in self.selection:
            year = datetime.datetime.strptime(item[3], "%Y-%m-%dT%H:%M:%SZ").year
            sub_folders.add(str(year))
        for item in sub_folders:
            if not os.path.exists(PATH_TO_IMAGES_STORAGE + item):
                try:
                    os.makedirs(PATH_TO_IMAGES_STORAGE + item)
                except OSError as err:
                    logging.error(f"Fail to create folder, {err}")
                    raise
                logging.info(f"Folder {PATH_TO_IMAGES_STORAGE + item} has been created.")
            if not os.path.exists(PATH_TO_VIDEOS_STORAGE + item):
                try:
                    os.makedirs(PATH_TO_VIDEOS_STORAGE + item)
                except OSError as err:
                    logging.error(f"Fail to create folder, {err}")
                    raise
                logging.info(f"Folder {PATH_TO_VIDEOS_STORAGE + item} has been created.")

    def get_media_items(self, auth):
        for item in self.selection:
            media_item = MediaItem(item[0], item[1], item[2], item[3])
            try:
                media_item.update_base_url(auth)
            except FileNotFoundError:
                logging.warning(f"Item {item[2]} not found on the server, removing from database.")
                media_item.remove_metadata_from_db()
            except SessionNotAuth:
                auth.refresh_access_token()
                media_item.update_base_url(auth)
            except Exception:
                logging.error(f'Fail to update base url by {item[2]}.')
                raise
            try:
                media_item.download()
            except FileExistsError:
                continue
            except Exception:
                logging.error(f'Fail to download {item[2]}.')
                raise


class Paginator:
    def __init__(self, auth):
        self.auth = auth
        self.listing = Listing()
        self.listing.check_mode()
        self.list_retrieved = False

    def get_whole_media_list(self):
        page = 0
        while True:
            next_page_token = self.listing.new_next_page_token
            try:
                self.listing.get_page(self.auth, next_page_token)
            except SessionNotAuth:
                self.auth.refresh_access_token()
                continue
            except NoItemsInResp or NoNextPageTokenInResp:
                self.list_retrieved = True
            except FailGettingPage:
                break
            if self.listing.current_mode == '0':
                self.listing.write_metadata()
            elif self.listing.current_mode == '1':
                try:
                    self.listing.write_metadata(mode='write_latest')
                except ObjAlreadyExists:
                    break
            else:
                logging.error('Unexpected error.')
                raise Exception()
            page += 1
            logging.info(f'{page} - processed.')
            if self.list_retrieved:
                DB_CONNECTION.execute("UPDATE account_info SET value='1' WHERE key='list_received'")
                DB.commit()
                logging.warning("List has been retrieved.")
                break


def main():
    authorization = Authorization()
    try:
        authorization.get_tokens()
    except FileNotFoundError:
        try:
            authorization.authenticate()
        except Exception as err:
            logging.error(f"Fail to authenticate, {err}")
            exit(5)
    except OSError as err:
        logging.error(f"Fail to authenticate, {err}")
        exit(6)
    except KeyError:
        logging.error("Fail to authenticate.")
        exit(4)
    logging.info('Started.')
    paginator = Paginator(authorization)
    try:
        paginator.get_whole_media_list()
    except Exception as err:
        logging.error(f"Unexpected error, {err}")
        DB.close()
        exit(1)
    logging.info('Start downloading a list of media items.')
    downloader = Downloader()
    try:
        downloader.create_tree()
    except OSError:
        logging.error("Please check storage paths in config.")
        exit(2)
    try:
        downloader.get_media_items(authorization)
    except Exception as err:
        logging.error(f"Fail to download media: {err}")
        exit(3)
    finally:
        DB.close()
    logging.info('Finished.')


if __name__ == '__main__':
    main()
