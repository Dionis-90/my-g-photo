#!/usr/bin/env python3.7

# This is a script that gets, downloads media files and metadata from your Google Photo storage to your local storage.

import sqlite3
import datetime
import time
from threading import Thread
from authorization import *

# Define constants
SRV_ENDPOINT = 'https://photoslibrary.googleapis.com/v1/'
DB = sqlite3.connect(DB_FILE_PATH)


class MediaItemsMetadata:
    def __init__(self):
        self.metadata = []
        cur_db_connection = DB.cursor()
        cur_db_connection.execute("INSERT OR IGNORE INTO account_info (key, value) VALUES ('list_received', '0')")
        DB.commit()
        cur_db_connection.execute("SELECT value FROM account_info WHERE key='list_received'")
        list_received_status = cur_db_connection.fetchone()[0]
        if list_received_status == '0':
            self.mode = 'write_all'
        elif list_received_status == '1':
            self.mode = 'write_latest'
        else:
            logging.error(f'Unexpected error.')

    def get_from_server(self, auth) -> int:
        logging.info('Start retrieving a list of media items.')
        objects_count_on_page = '100'
        url = SRV_ENDPOINT+'mediaItems'
        next_page_token = None
        page = 0
        while stop == 0:
            params = {'key': API_KEY,
                      'pageSize': objects_count_on_page,
                      'pageToken': next_page_token}
            headers = {'Accept': 'application/json',
                       'Authorization': 'Bearer ' + auth.get_access_token()}
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 401:
                auth.refresh_access_token()
                continue
            elif response.status_code != 200:
                logging.warning(f"http code {response.status_code} when trying to get page of list with "
                                f"next_page_token: {next_page_token}, response: {response.text}")
                return 21
            try:
                self.metadata.extend(response.json()['mediaItems'])
            except KeyError:
                logging.warning(f"No mediaItems object in response. Response: {response.text}")
                return 22
            try:
                next_page_token = response.json()['nextPageToken']
            except KeyError:
                logging.warning("No nextPageToken object in response. Probably got end of the list.")
                return 23
            page += 1
            logging.info(f"{page} pages processed.")

    def write_to_db(self):
        db = sqlite3.connect(DB_FILE_PATH)
        cur_db_connection = db.cursor()
        items = 0
        for item in self.metadata:
            values = (item['id'], item['filename'], item['mimeType'], item['mediaMetadata']['creationTime'])
            try:
                cur_db_connection.execute('INSERT INTO my_media (object_id, filename, media_type, creation_time) \
                    VALUES (?, ?, ?, ?)', values)
            except sqlite3.IntegrityError:
                if self.mode == 'write_latest':
                    logging.info("Latest media items metadata wrote.")
                    return 10
                elif self.mode == 'write_all':
                    logging.info(f"Media item {item['filename']} already in the list.")
                    continue
                else:
                    logging.error("Unexpected error.")
                    return 20
            finally:
                db.commit()
            items += 1
            logging.info(f"{items} items processed.")
        cur_db_connection.execute("UPDATE account_info SET value='1' WHERE key='list_received'")
        db.commit()
        db.close()
        logging.warning("List has been retrieved.")
        return 0


class MediaFile:
    def __init__(self, metadata):
        self.creation_time = metadata[0]
        self.object_id = metadata[1]
        self.filename = metadata[2]
        self.media_type = metadata[3]
        self.creation_year = datetime.datetime.strptime(self.creation_time, "%Y-%m-%dT%H:%M:%SZ").year
        self.base_url = ''

    def get_base_url(self, auth) -> int:
        headers = {'Accept': 'application/json',
                   'Authorization': 'Bearer ' + auth.get_access_token()}
        params = {'key': API_KEY}
        response = requests.get(SRV_ENDPOINT +'mediaItems/' + self.object_id, params=params, headers=headers)
        if response.status_code == 401:
            logging.error("Unauthorized.")
            return 1
        elif response.status_code == 404:
            logging.warning(f"Item {self.object_id} not found on the server, removing from database.")
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
        logging.info('Start downloading a list of media items.')
        cur_db_connection = DB.cursor()
        if 'image' in self.media_type:
            response = requests.get(self.base_url+'=d', params=None, headers=None)
        elif 'video' in self.media_type:
            response = requests.get(self.base_url+'=dv', params=None, headers=None, stream=True)
        else:
            logging.error('Unexpected error.')
            return 1
        sub_folder_name = str(self.creation_year)+'/'
        if 'text/html' in response.headers['Content-Type']:
            logging.error(f"Error. Server returns: {response.text}")
            return 2
        elif 'image' in response.headers['Content-Type']:
            os.makedirs(PATH_TO_IMAGES_STORAGE+sub_folder_name, exist_ok=True)
            if os.path.exists(PATH_TO_IMAGES_STORAGE + sub_folder_name + self.filename):
                logging.warning(f"File {self.filename} already exist in local storage! Setting 'stored = 2' "
                                f"in database.")
                cur_db_connection.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (self.object_id,))
                DB.commit()
                return 3
            media_file = open(PATH_TO_IMAGES_STORAGE + sub_folder_name + self.filename, 'wb')
            media_file.write(response.content)
            media_file.close()
        elif 'video' in response.headers['Content-Type']:
            os.makedirs(PATH_TO_VIDEOS_STORAGE+sub_folder_name, exist_ok=True)
            if os.path.exists(PATH_TO_VIDEOS_STORAGE + sub_folder_name + self.filename):
                logging.warning(f"File {self.filename} already exist in local storage! Setting 'stored = 2' "
                                f"in database.")
                cur_db_connection.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (self.object_id,))
                DB.commit()
                return 4
            media_file = open(PATH_TO_VIDEOS_STORAGE + sub_folder_name + self.filename, 'wb')
            for chunk in response.iter_content(chunk_size=1024):
                media_file.write(chunk)
            media_file.close()
        else:
            logging.error('Unexpected error.')
            return 5
        logging.info(f"Media file {self.filename} stored.")
        cur_db_connection.execute("UPDATE my_media SET stored='1' WHERE object_id=?", (self.object_id,))
        DB.commit()
        return 0


def main():
    # Checking required paths.
    if not os.path.exists(IDENTITY_FILE_PATH):
        print(f"File {IDENTITY_FILE_PATH} does not exist! Please put the file in working directory.")
        exit(1)
    if not os.path.exists(PATH_TO_VIDEOS_STORAGE):
        print(f"Path {PATH_TO_VIDEOS_STORAGE} does not exist! Please set correct path.")
        exit(2)
    if not os.path.exists(PATH_TO_IMAGES_STORAGE):
        print(f"Path {PATH_TO_IMAGES_STORAGE} does not exist! Please set correct path.")
        exit(3)
    logging.info('Started.')
    authorization = Authorization()
    media_items_metadata = MediaItemsMetadata()
    thread1 = Thread(target=media_items_metadata.get_from_server, args=(authorization,))
    thread2 = Thread(target=media_items_metadata.write_to_db)
    thread1.start()
    time.sleep(10)
    thread2.start()
    thread1.join()
    thread2.join()
    DB.close()
    logging.info('Finished.')
    exit()


def get_list_one_page(next_page_token, authorization, mode='fetch_all') -> tuple:
    """
    Gets one page of media objects list and puts metadata into the database.
    :param
        mode: 'fetch_all' - if media object meta info already in the DB tries to get whole page and returns 0,
              'fetch_last' - returns 10 immediately if media object meta info already in the DB,
        next_page_token: We receive this token in response after successful execution of this function.
                         At the first run we need to set this as None.
    :return: (exit_code, next_page_token):
        exit_codes:
             0 - Got page of list and nextPageToken successfully.
            10 - Media object metadata already exists in database.
            20 - Unexpected error.
            21 - Not http 200 code when trying to get page of list.
            22 - No mediaItems object in response.
            23 - No nextPageToken object in response.
            30 - http 401 code, the token may have expired.
    """


def get_media_files(authorization) -> int:
    """
    Downloads media files to media folders and marks 1 in 'stored' field.
    If file already exist, marks it 2 in 'stored' field.
    :return:
        0 - success.
        1 or 3 - unexpected error.
        2 - server returns a text.
        4 - http 401 code, the token may have expired.
    """
    cur_db_connection = DB.cursor()
    cur_db_connection.execute("SELECT object_id, filename, media_type, creation_time FROM my_media WHERE stored = '0'")
    selection = cur_db_connection.fetchall()

    def create_sub_folders_in_storage():
        sub_folders = set()
        for item in selection:
            year = datetime.datetime.strptime(item[0], "%Y-%m-%dT%H:%M:%SZ").year
            sub_folders.add(str(year))
        for item in sub_folders:
            if not os.path.exists(PATH_TO_IMAGES_STORAGE + item):
                os.makedirs(PATH_TO_IMAGES_STORAGE + item)
                logging.info(f"Folder {PATH_TO_IMAGES_STORAGE + item} has been created.")
            if not os.path.exists(PATH_TO_VIDEOS_STORAGE + item):
                os.makedirs(PATH_TO_VIDEOS_STORAGE + item)
                logging.info(f"Folder {PATH_TO_VIDEOS_STORAGE + item} has been created.")

    create_sub_folders_in_storage()

    headers = {'Accept': 'application/json',
               'Authorization': 'Bearer ' + authorization.get_access_token()}
    params = {'key': API_KEY}
    for item in selection:
        response = requests.get(SRV_ENDPOINT+'mediaItems/'+item[0], params=params, headers=headers)
        if response.status_code == 401:
            return 4
        elif response.status_code == 404:
            logging.warning(f"Item {item[1]} not found on the server, removing from database.")
            cur_db_connection.execute("DELETE FROM my_media WHERE object_id=?", (item[0],))
            DB.commit()
            continue
        base_url = response.json()['baseUrl']


def main_old():
    while True:
        result = get_media_files(auth)
        if result == 4:
            auth.refresh_access_token()
        elif result == 0:
            logging.info("All media items stored.")
            break
        elif result != 0:
            logging.error(f"Application error. Returns code - {result}.")
            exit(6)
        else:
            logging.error(f'Unexpected error. Returns code - {result}.')
            exit(7)


if __name__ == '__main__':
    main()
