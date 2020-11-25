#!/usr/bin/env python3.7

import json
import os
import requests
import sqlite3
import logging
import datetime
import webbrowser
from config import *
from oauth2client import file, client, tools

# Define constants
SRV_ENDPOINT = 'https://photoslibrary.googleapis.com/v1/'
SCOPES = ['https://www.googleapis.com/auth/photoslibrary',
          'https://www.googleapis.com/auth/photoslibrary.edit.appcreateddata',
          'https://www.googleapis.com/auth/photoslibrary.sharing']
DB = sqlite3.connect(DB_FILE_PATH)

# Logging global config
logging.basicConfig(format='%(asctime)s %(levelname)s %(funcName)s: %(message)s',
                    filename=LOG_FILE_PATH, filemode='a', level=logging.INFO)


def get_auth() -> int:
    with open(IDENTITY_FILE_PATH) as f:
        identity = json.load(f)['installed']
    redirect_uri = identity['redirect_uris'][0]
    scope = ''
    for i in SCOPES:
        scope += i+'%20'
    url = f"{identity['auth_uri']}?scope={scope}&response_type=code&\
redirect_uri={redirect_uri}&client_id={identity['client_id']}"
    print(f"If you do not have local browser please visit url: {url}")
    webbrowser.open(url, new=0, autoraise=True)
    code = input("Please enter the code: ")
    token_uri = identity['token_uri']
    data = {'code': code,
            'client_id': identity['client_id'],
            'client_secret': identity['client_secret'],
            'redirect_uri': redirect_uri,
            'grant_type': 'authorization_code'}
    r = requests.post(token_uri, data=data)
    if r.status_code != 200:
        logging.error(f"Not authenticated. http code: {r.status_code}, response: {r.text}.")
        return 1
    with open(OAUTH2_FILE_PATH, 'w') as f:
        json.dump(r.json(), f)
    logging.info("Authenticated successfully.")
    return 0


def get_auth_old():
    store = file.Storage(OAUTH2_FILE_PATH)
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets(IDENTITY_FILE_PATH, SCOPES)
        tools.run_flow(flow, store)
    if not os.path.exists(OAUTH2_FILE_PATH):
        print(f"File {OAUTH2_FILE_PATH} does not exist! Authentication unsuccessful.")
        exit(1)


def read_access_token() -> str:
    if os.path.exists(ACCESS_TOKEN_FILE):
        with open(ACCESS_TOKEN_FILE) as f:
            new_access_token = json.load(f)['access_token']
    elif os.path.exists(OAUTH2_FILE_PATH):
        with open(OAUTH2_FILE_PATH) as f:
            new_access_token = json.load(f)['access_token']
    else:
        logging.error("Not authenticated.")
        exit(1)
    return new_access_token


def refr_token():
    with open(OAUTH2_FILE_PATH) as f:
        oauth2_file_data = json.load(f)
    with open(IDENTITY_FILE_PATH) as f:
        identity_file_data = json.load(f)['installed']
    values = {'client_id': identity_file_data['client_id'],
              'client_secret': identity_file_data['client_secret'],
              'refresh_token': oauth2_file_data['refresh_token'],
              'grant_type': 'refresh_token'}
    response = requests.post(identity_file_data['token_uri'], data=values)
    with open(ACCESS_TOKEN_FILE, 'w') as f:
        json.dump(response.json(), f)
    logging.info('Token has been refreshed.')


def get_list_one_page(next_page_token, mode='fetch_all') -> tuple:
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
               'Authorization': 'Bearer '+read_access_token()}
    params = {'key': API_KEY,
              'pageSize': objects_count_on_page,
              'pageToken': next_page_token}
    cur_db_connection = DB.cursor()
    r = requests.get(url, params=params, headers=headers)
    if r.status_code == 401:
        return 30, next_page_token
    elif r.status_code != 200:
        logging.warning(f"http code {r.status_code} when trying to get page of list with next_page_token: \
                        {next_page_token}, response: {r.text}")
        return 21, next_page_token

    try:
        media_items = r.json()['mediaItems']
    except KeyError:
        logging.warning(f"No mediaItems object in response. Response: {r.text}")
        return 22, next_page_token

    try:
        new_next_page_token = r.json()['nextPageToken']
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


def get_media_files() -> int:
    """
    Downloads media files to media folders and marks 1 in 'stored' field.
    If file already exist, marks it 2 in 'stored' field.
    :return:
        0 - success.
        1 or 2 or 3 - unexpected error.
        4 - http 401 code, the token may have expired.
    """
    cur_db_connection = DB.cursor()
    cur_db_connection.execute("SELECT object_id, filename, media_type, creation_time FROM my_media WHERE stored = '0'")
    selection = cur_db_connection.fetchall()
    headers = {'Accept': 'application/json',
               'Authorization': 'Bearer ' + read_access_token()}
    params = {'key': API_KEY}

    for item in selection:
        r = requests.get(SRV_ENDPOINT+'mediaItems/'+item[0], params=params, headers=headers)
        if r.status_code == 401:
            return 4
        elif r.status_code == 404:
            logging.warning(f"Item {item[1]} not found on the server, removing from database.")
            cur_db_connection.execute("DELETE FROM my_media WHERE object_id=?", (item[0],))
            DB.commit()
            continue
        base_url = r.json()['baseUrl']
        if 'image' in item[2]:
            r = requests.get(base_url+'=d', params=None, headers=None)
        elif 'video' in item[2]:
            r = requests.get(base_url+'=dv', params=None, headers=None)
        else:
            logging.error('Unexpected error.')
            return 1
        year_of_item = datetime.datetime.strptime(item[3], "%Y-%m-%dT%H:%M:%SZ").year
        subfolder_name = str(year_of_item)+'/'
        if 'text/html' in r.headers['Content-Type']:
            logging.error(f"Unexpected error: {r.text}")
            return 2
        elif 'image' in r.headers['Content-Type']:
            if os.path.exists(PATH_TO_IMAGES_STORAGE+subfolder_name+item[1]):
                logging.warning(f"File {item[1]} already exist in local storage! Setting 'stored = 2' in database.")
                cur_db_connection.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (item[0],))
                DB.commit()
                continue
            f = open(PATH_TO_IMAGES_STORAGE+subfolder_name+item[1], 'wb')
            f.write(r.content)
            f.close()
        elif 'video' in r.headers['Content-Type']:
            if os.path.exists(PATH_TO_VIDEOS_STORAGE+subfolder_name+item[1]):
                logging.warning(f"File {item[1]} already exist in local storage! Setting 'stored = 2' in database.")
                cur_db_connection.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (item[0],))
                DB.commit()
                continue
            f = open(PATH_TO_VIDEOS_STORAGE+subfolder_name+item[1], 'wb')
            f.write(r.content)
            f.close()
        else:
            logging.error('Unexpected error.')
            return 3
        logging.info(f'Item {item[1]} stored.')
        cur_db_connection.execute("UPDATE my_media SET stored='1' WHERE object_id=?", (item[0],))
        DB.commit()
    return 0


def list_albums():  # TODO
    pass


def create_album(album_name) -> str:  # TODO
    print(album_name)
    album_id = ''
    return album_id


def add_to_album(album_id, item_id) -> int:  # TODO
    print(album_id, item_id)
    return 0


def share_album(album_id) -> str:  # TODO
    print(album_id)
    url = ''
    return url


def create_subfolders_in_storage():
    cur_db_connection = DB.cursor()
    cur_db_connection.execute("SELECT creation_time FROM my_media WHERE stored = '0'")
    selection = cur_db_connection.fetchall()
    subfolders = set()
    for item in selection:
        year = datetime.datetime.strptime(item[0], "%Y-%m-%dT%H:%M:%SZ").year
        subfolders.add(str(year))
    for item in subfolders:
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
    if not os.path.exists(OAUTH2_FILE_PATH):
        auth_result = get_auth()
        if auth_result != 0:
            exit(1)

    logging.info('Start retrieving a list of media items.')

    create_subfolders_in_storage()

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
            result = get_list_one_page(result[1], mode='fetch_all')
        elif list_received_status == '1':
            result = get_list_one_page(result[1], mode='fetch_last')
        else:
            logging.error(f'Unexpected error. Returns code - {result[0]}.')
            exit(1)

        if result[0] == 30:
            refr_token()
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
        result = get_media_files()
        if result == 4:
            refr_token()
        elif result == 0:
            logging.info("All media items stored.")
            break
        elif result != 0:
            logging.error(f"Application error. Returns code - {result}.")
            exit(1)
        else:
            logging.error(f'Unexpected error. Returns code - {result}.')
            exit(1)
    logging.info('Finished.')


if __name__ == '__main__':
    main()

DB.close()
