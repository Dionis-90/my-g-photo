#!/usr/bin/env python3.7

# This is a script that gets, downloads media files and metadata from your Google Photo storage to your local storage.

import sqlite3
import datetime
from authorization import *

# Define constants
SRV_ENDPOINT = 'https://photoslibrary.googleapis.com/v1/'
DB = sqlite3.connect(DB_FILE_PATH)


class MediaItem:
    def __init__(self, metadata):
        self.metadata = metadata
        self.id = metadata['id']
        self.base_url = metadata['baseUrl']
        self.mime_type = metadata['mimeType']
        self.filename = metadata['filename']
        self.creation_time = metadata['mediaMetadata']['creationTime']
        self.creation_year = datetime.datetime.strptime(self.creation_time, "%Y-%m-%dT%H:%M:%SZ").year
        self.width = metadata['mediaMetadata']['width']
        self.height = metadata['mediaMetadata']['height']

    def write_metadata_to_db(self) -> int:
        cur_db_connection = DB.cursor()
        values = (self.id, self.filename, self.mime_type, self.creation_time)
        try:
            cur_db_connection.execute('INSERT INTO my_media (object_id, filename, media_type, creation_time) \
                    VALUES (?, ?, ?, ?)', values)
            DB.commit()
        except sqlite3.IntegrityError:
            logging.info(f"Media item {self.filename} already in the list.")
            return 50
        return 0

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
        logging.info('Start downloading a list of media items.')
        cur_db_connection = DB.cursor()
        if 'image' in self.mime_type:
            response = requests.get(self.base_url+'=d', params=None, headers=None)
        elif 'video' in self.mime_type:
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
                cur_db_connection.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (self.id,))
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
                cur_db_connection.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (self.id,))
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
        cur_db_connection.execute("UPDATE my_media SET stored='1' WHERE object_id=?", (self.id,))
        DB.commit()
        return 0


class Listing:
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

    def write_metadata(self, mode='write_all') -> int:
        """
        :param mode: 'write_all' or 'write_latest'
        :return:
        """
        logging.info(f'Running in mode {mode}.')
        for item in self.list_one_page:
            media_item = MediaItem(item)
            result = media_item.write_metadata_to_db()
            if result == 50 and mode == 'write_all':
                continue
            elif result == 50 and mode == 'write_latest':
                return 51
        return 0

    def check_mode(self):
        cur_db_connection = DB.cursor()
        cur_db_connection.execute("INSERT OR IGNORE INTO account_info (key, value) VALUES ('list_received', '0')")
        DB.commit()
        cur_db_connection.execute("SELECT value FROM account_info WHERE key='list_received'")
        self.current_mode = cur_db_connection.fetchone()[0]


def main():
    # Checking required paths. TODO: do it as exceptions.
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
    cur_db_connection = DB.cursor()
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
            paginator_output = paginator.write_metadata()
        elif paginator.current_mode == '1':
            paginator_output = paginator.write_metadata(mode='write_latest')
        else:
            logging.error('Unexpected error.')
            exit(1)
        if paginator_output == 51:
            break
        page += 1
        logging.info(f'{page} - processed.')
        if list_retrieved:
            cur_db_connection.execute("UPDATE account_info SET value='1' WHERE key='list_received'")
            DB.commit()
            logging.warning("List has been retrieved.")
            break
    DB.close()
    logging.info('Finished.')
    exit()


'''
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
'''

if __name__ == '__main__':
    main()
