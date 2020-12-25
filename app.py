#!/usr/bin/env python3.7

# This is a script that gets, downloads media files and metadata from your Google Photo storage to your local storage.

import sqlite3
import datetime
from authorization import *

# Define constants
SRV_ENDPOINT = 'https://photoslibrary.googleapis.com/v1/'
DB = sqlite3.connect(DB_FILE_PATH)


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
    logging.info(f"Function running in {mode} mode.")
    objects_count_on_page = '100'
    url = SRV_ENDPOINT+'mediaItems'
    headers = {'Accept': 'application/json',
               'Authorization': 'Bearer '+authorization.get_access_token()}
    params = {'key': API_KEY,
              'pageSize': objects_count_on_page,
              'pageToken': next_page_token}
    cur_db_connection = DB.cursor()
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 401:
        return 30, next_page_token
    elif response.status_code != 200:
        logging.warning(f"http code {response.status_code} when trying to get page of list with next_page_token: \
                        {next_page_token}, response: {response.text}")
        return 21, next_page_token

    try:
        media_items = response.json()['mediaItems']
    except KeyError:
        logging.warning(f"No mediaItems object in response. Response: {response.text}")
        return 22, next_page_token

    try:
        new_next_page_token = response.json()['nextPageToken']
    except KeyError:
        logging.warning("No nextPageToken object in response. Probably end of the list.")
        new_next_page_token = None
        return 23, new_next_page_token

    for item in media_items:
        values = (item['id'], item['filename'], item['mimeType'], item['mediaMetadata']['creationTime'])
        try:
            cur_db_connection.execute('INSERT INTO my_media (object_id, filename, media_type, creation_time) \
            VALUES (?, ?, ?, ?)', values)
        except sqlite3.IntegrityError:
            if mode == 'fetch_last':
                return 10, new_next_page_token
            elif mode == 'fetch_all':
                logging.info(f"Media item {item['filename']} already in the list.")
                continue
            else:
                logging.error("Unexpected error.")
                return 20, new_next_page_token
        finally:
            DB.commit()
    return 0, new_next_page_token


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
        if 'image' in item[2]:
            response = requests.get(base_url+'=d', params=None, headers=None)
        elif 'video' in item[2]:
            response = requests.get(base_url+'=dv', params=None, headers=None, stream=True)
        else:
            logging.error('Unexpected error.')
            return 1
        year_of_item = datetime.datetime.strptime(item[3], "%Y-%m-%dT%H:%M:%SZ").year
        sub_folder_name = str(year_of_item)+'/'
        if 'text/html' in response.headers['Content-Type']:
            logging.error(f"Error. Server returns: {response.text}")
            return 2
        elif 'image' in response.headers['Content-Type']:
            if os.path.exists(PATH_TO_IMAGES_STORAGE+sub_folder_name+item[1]):
                logging.warning(f"File {item[1]} already exist in local storage! Setting 'stored = 2' in database.")
                cur_db_connection.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (item[0],))
                DB.commit()
                continue
            media_file = open(PATH_TO_IMAGES_STORAGE+sub_folder_name+item[1], 'wb')
            media_file.write(response.content)
            media_file.close()
        elif 'video' in response.headers['Content-Type']:
            if os.path.exists(PATH_TO_VIDEOS_STORAGE+sub_folder_name+item[1]):
                logging.warning(f"File {item[1]} already exist in local storage! Setting 'stored = 2' in database.")
                cur_db_connection.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (item[0],))
                DB.commit()
                continue
            media_file = open(PATH_TO_VIDEOS_STORAGE+sub_folder_name+item[1], 'wb')
            for chunk in response.iter_content(chunk_size=1024):
                media_file.write(chunk)
            media_file.close()
        else:
            logging.error('Unexpected error.')
            return 3
        logging.info(f'Media file {item[1]} stored.')
        cur_db_connection.execute("UPDATE my_media SET stored='1' WHERE object_id=?", (item[0],))
        DB.commit()
    return 0


def create_sub_folders_in_storage():
    cur_db_connection = DB.cursor()
    cur_db_connection.execute("SELECT creation_time FROM my_media WHERE stored = '0'")
    selection = cur_db_connection.fetchall()
    sub_folders = set()
    for item in selection:
        year = datetime.datetime.strptime(item[0], "%Y-%m-%dT%H:%M:%SZ").year
        sub_folders.add(str(year))
    for item in sub_folders:
        if not os.path.exists(PATH_TO_IMAGES_STORAGE+item):
            os.makedirs(PATH_TO_IMAGES_STORAGE+item)
            logging.info(f"Folder {PATH_TO_IMAGES_STORAGE+item} has been created.")
        if not os.path.exists(PATH_TO_VIDEOS_STORAGE+item):
            os.makedirs(PATH_TO_VIDEOS_STORAGE+item)
            logging.info(f"Folder {PATH_TO_VIDEOS_STORAGE + item} has been created.")


def main():
    logging.info('Started.')

    # Checking required paths.
    if not os.path.exists(IDENTITY_FILE_PATH):
        print(f"File {IDENTITY_FILE_PATH} does not exist! Please put the file in working directory.")
        exit(1)
    if not os.path.exists(PATH_TO_VIDEOS_STORAGE):
        print(f"Path {PATH_TO_VIDEOS_STORAGE} does not exist! Please set correct path.")
        exit(1)
    if not os.path.exists(PATH_TO_IMAGES_STORAGE):
        print(f"Path {PATH_TO_IMAGES_STORAGE} does not exist! Please set correct path.")
        exit(1)
    authorization = Authorization()
    logging.info('Start retrieving a list of media items.')

    create_sub_folders_in_storage()

    # Get list of media and write meta information into the DB (pagination).
    cur_db_connection = DB.cursor()
    cur_db_connection.execute("INSERT OR IGNORE INTO account_info (key, value) VALUES ('list_received', '0')")
    DB.commit()
    result = (0, None)
    page = 0
    while True:
        cur_db_connection.execute("SELECT value FROM account_info WHERE key='list_received'")
        list_received_status = cur_db_connection.fetchone()[0]
        if list_received_status == '0':
            result = get_list_one_page(result[1], authorization, mode='fetch_all')
        elif list_received_status == '1':
            result = get_list_one_page(result[1], authorization, mode='fetch_last')
        else:
            logging.error(f'Unexpected error. Returns code - {result[0]}.')
            exit(1)

        if result[0] == 30:
            # refresh_access_token()
            authorization.refresh_access_token()
            continue
        elif result[0] == 10:
            logging.warning("List of media items retrieved.")
            break
        elif result[0] == 22 or result[0] == 23:
            cur_db_connection.execute("UPDATE account_info SET value='1' WHERE key='list_received'")
            DB.commit()
            logging.warning("List has been retrieved.")
            break
        elif result[0] != 0:
            logging.error(f"Application error. Returns code - {result[0]}.")
            DB.close()
            exit(1)
        page += 1
        logging.info(f"Page N {page} processed.")

    # Download media files to media folder.
    logging.info('Start downloading a list of media items.')
    while True:
        result = get_media_files(authorization)
        if result == 4:
            authorization.refresh_access_token()
        elif result == 0:
            logging.info("All media items stored.")
            break
        elif result != 0:
            logging.error(f"Application error. Returns code - {result}.")
            exit(1)
        else:
            logging.error(f'Unexpected error. Returns code - {result}.')
            exit(1)

    DB.close()
    logging.info('Finished.')


if __name__ == '__main__':
    main()
