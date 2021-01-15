#!/usr/bin/env python3.7

# This is a script that gets, downloads media files and metadata from your Google Photo storage to your local storage.

import sqlite3
import datetime
from authorization import *

# Define constants
SRV_ENDPOINT = 'https://photoslibrary.googleapis.com/v1/'
DB = sqlite3.connect(DB_FILE_PATH)
DB_CONNECTION = DB.cursor()


class MediaItem:
    """Creates media item object."""
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

    def remove_metadata_from_db(self):
        try:
            DB_CONNECTION.execute("DELETE FROM my_media WHERE object_id=?", (self.id,))
            DB.commit()
        except sqlite3.Error as err:
            logging.error(f'Fail to remove {self.filename} from the DB. Error {err}')

    def update_base_url(self, auth) -> int:
        headers = {'Accept': 'application/json',
                   'Authorization': 'Bearer ' + auth.get_access_token()}
        params = {'key': API_KEY}
        response = requests.get(SRV_ENDPOINT+'mediaItems/'+self.id, params=params, headers=headers)
        if response.status_code == 401:
            logging.error("Unauthorized.")
            return 1
        elif response.status_code == 404:
            logging.warning(f"Item {self.id} not found on the server.")
            return 2
        elif response.status_code != 200:
            logging.error(f'Response code: {response.status_code}. Response: {response.text}')
            return 3
        try:
            self.base_url = response.json()['baseUrl']
        except KeyError:
            logging.error(f'Response does not contain baseUrl. Response: {response.text}')
            return 4
        return 0

    def download(self) -> int:
        sub_folder_name = str(self.creation_year)+'/'

        def download_photo_item() -> int:
            response = requests.get(self.base_url+'=d', params=None, headers=None)
            if 'text/html' in response.headers['Content-Type']:
                logging.error(f"Error. Server returns: {response.text}")
                return 2
            elif 'image' in response.headers['Content-Type']:
                if os.path.exists(PATH_TO_IMAGES_STORAGE + sub_folder_name + self.filename):
                    logging.warning(f"File {self.filename} already exist in local storage! Setting 'stored = 2' "
                                    f"in database.")
                    DB_CONNECTION.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (self.id,))
                    DB.commit()
                    return 3
                media_file = open(PATH_TO_IMAGES_STORAGE + sub_folder_name + self.filename, 'wb')  # TODO:use try-except
                media_file.write(response.content)
                media_file.close()
            else:
                logging.error('Unexpected content type.')
                return 5
            logging.info(f"Media file {self.filename} stored.")
            DB_CONNECTION.execute("UPDATE my_media SET stored='1' WHERE object_id=?", (self.id,))
            DB.commit()
            return 0

        def download_video_item() -> int:
            response = requests.get(self.base_url+'=dv', params=None, headers=None, stream=True)
            if 'text/html' in response.headers['Content-Type']:
                logging.error(f"Error. Server returns: {response.text}")
                return 2
            if 'video' in response.headers['Content-Type']:
                if os.path.exists(PATH_TO_VIDEOS_STORAGE + sub_folder_name + self.filename):
                    logging.warning(f"File {self.filename} already exist in local storage! Setting 'stored = 2' "
                                    f"in database.")
                    DB_CONNECTION.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (self.id,))
                    DB.commit()
                    return 4
                media_file = open(PATH_TO_VIDEOS_STORAGE + sub_folder_name + self.filename, 'wb')  # TODO:use try-except
                for chunk in response.iter_content(chunk_size=1024):
                    media_file.write(chunk)
                media_file.close()
            else:
                logging.error('Unexpected content type.')
                return 5
            logging.info(f"Media file {self.filename} stored.")
            DB_CONNECTION.execute("UPDATE my_media SET stored='1' WHERE object_id=?", (self.id,))
            DB.commit()
            return 0

        if 'image' in self.mime_type:
            result = download_photo_item()
        elif 'video' in self.mime_type:
            result = download_video_item()
        else:
            logging.error('Unexpected mime type.')
            return 1
        return result


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
                   'Authorization': 'Bearer ' + auth.get_access_token()}
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 401:
            auth.refresh_access_token()
            return 20
        elif response.status_code != 200:
            logging.warning(f"http code {response.status_code} when trying to get page of list with "
                            f"next_page_token: {next_page_token}, response: {response.text}")
            return 21
        try:
            self.list_one_page = response.json()['mediaItems']
        except KeyError:
            logging.warning(f"No mediaItems object in response. Response: {response.text}")
            return 22
        try:
            self.new_next_page_token = response.json()['nextPageToken']
        except KeyError:
            logging.warning("No nextPageToken object in response. Probably got end of the list.")
            self.new_next_page_token = None
            return 23
        return 0

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
                    exit(5)

    def check_mode(self):
        try:
            DB_CONNECTION.execute("INSERT OR IGNORE INTO account_info (key, value) VALUES ('list_received', '0')")
            DB.commit()
            DB_CONNECTION.execute("SELECT value FROM account_info WHERE key='list_received'")
        except sqlite3.Error as err:
            logging.error(f'DB query failed, {err}.')
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
                os.makedirs(PATH_TO_IMAGES_STORAGE + item)
                logging.info(f"Folder {PATH_TO_IMAGES_STORAGE + item} has been created.")
            if not os.path.exists(PATH_TO_VIDEOS_STORAGE + item):
                os.makedirs(PATH_TO_VIDEOS_STORAGE + item)
                logging.info(f"Folder {PATH_TO_VIDEOS_STORAGE + item} has been created.")

    def get_media_items(self, auth) -> int:
        for item in self.selection:
            media_item = MediaItem(item[0], item[1], item[2], item[3])
            result = media_item.update_base_url(auth)
            if result == 2:
                logging.warning(f"Item {item[2]} not found on the server, removing from database.")
                media_item.remove_metadata_from_db()
            elif result != 0:
                logging.error(f'Fail to update base url by {item[2]}.')
                return result
            result = media_item.download()
            if result == 3 or result == 4:
                continue
            elif result != 0:
                logging.error(f'Fail to download {item[2]}, returns {result}.')
                return result
        return 0


class ObjAlreadyExists(Exception):
    def __init__(self, message):
        logging.info(message)


def main():
    # Checking required paths. TODO: do it as exceptions.
    if not os.path.exists(PATH_TO_VIDEOS_STORAGE):
        print(f"Path {PATH_TO_VIDEOS_STORAGE} does not exist! Please set correct path.")
        exit(2)
    if not os.path.exists(PATH_TO_IMAGES_STORAGE):
        print(f"Path {PATH_TO_IMAGES_STORAGE} does not exist! Please set correct path.")
        exit(3)
    logging.info('Started.')
    authorization = Authorization()
    paginator = Listing()
    paginator.check_mode()
    page = 0
    list_retrieved = False
    while True:
        next_page_token = paginator.new_next_page_token
        paginator_output = paginator.get_page(authorization, next_page_token)
        if paginator_output == 20:
            continue
        elif paginator_output == 22 or paginator_output == 23:
            list_retrieved = True
        elif paginator_output != 0:
            logging.error(f'Error. Code {paginator_output}.')
            break
        if paginator.current_mode == '0':
            paginator.write_metadata()
        elif paginator.current_mode == '1':
            try:
                paginator.write_metadata(mode='write_latest')
            except ObjAlreadyExists:
                break
        else:
            logging.error('Unexpected error.')
            exit(1)
        page += 1
        logging.info(f'{page} - processed.')
        if list_retrieved:
            DB_CONNECTION.execute("UPDATE account_info SET value='1' WHERE key='list_received'")
            DB.commit()
            logging.warning("List has been retrieved.")
            break
    logging.info('Start downloading a list of media items.')
    downloader = Downloader()
    downloader.create_tree()
    downloader.get_media_items(authorization)
    DB.close()
    logging.info('Finished.')


if __name__ == '__main__':
    main()
