#!/usr/bin/env python3.7

import fileinput
import json
import os
import requests
import sqlite3
import time
import logging
import datetime
from config import *
from oauth2client import file, client, tools

# TODO make auth with googleapiclient

# Define constants
SRV_ENDPOINT = 'https://photoslibrary.googleapis.com/v1/'
SCOPES = 'https://www.googleapis.com/auth/photoslibrary'


def get_auth():
    store = file.Storage(OAUTH2_FILE_PATH)
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets(IDENTITY_FILE_PATH, SCOPES)
        tools.run_flow(flow, store)
    if not os.path.exists(OAUTH2_FILE_PATH):
        print(f"File {OAUTH2_FILE_PATH} does not exist! Authentication unsuccessful.")
        exit(1)
    return


def read_credentials() -> json:
    with open(OAUTH2_FILE_PATH) as oauth2_file:
        oauth2_file_data = json.load(oauth2_file)
        return oauth2_file_data


def refr_token():
    values = {'client_id': credentials['client_id'],
              'client_secret': credentials['client_secret'],
              'refresh_token': credentials['refresh_token'],
              'grant_type': 'refresh_token'}
    response = requests.post(credentials['token_uri'], data=values)
    new_access_token = json.loads(response.text)['access_token']
    for line in fileinput.input(OAUTH2_FILE_PATH, inplace=True):
        print(line.replace(credentials['access_token'], new_access_token)),
    logging.info('Token has been refreshed.')
    return


def get_list_one_page(next_page_token) -> tuple:
    """
    Gets one page of media objects list and puts metadata into the database.
    :param next_page_token: We receive this token in response after successful execution of this function.
        At the first run we need to set this as None.
    :return: (exit_code, next_page_token):
         0 - got page of list successfully.
        10 - media object metadata already exists in database.
        21 - not http 200 code when trying to get page of list.
        22 - No mediaItems object in response.
        23 - No nextPageToken object in response.
        30 - http 401 code, the token may have expired.
    """
    objects_count_on_page = '100'
    url = SRV_ENDPOINT+'mediaItems'
    headers = {'Accept': 'application/json',
               'Authorization': 'Bearer ' + credentials['access_token']}
    params = {'key': API_KEY,
              'pageSize': objects_count_on_page,
              'pageToken': next_page_token}
    r = requests.get(url, params=params, headers=headers)
    if r.status_code == 401:
        return 30, next_page_token
    elif r.status_code != 200:
        logging.warning(f"http code {r.status_code} when trying to get page of list with next_page_token: \
                        {next_page_token}, response: {r.text}")
        return 21, next_page_token
    try:
        media_items = json.loads(r.text)['mediaItems']
    except KeyError:
        logging.warning(f"No mediaItems object in response. Response: {r.text}")
        return 22, next_page_token
    try:
        new_next_page_token = json.loads(r.text)['nextPageToken']
    except KeyError:
        logging.warning("No nextPageToken object in response. Probably end of the list.")
        new_next_page_token = None
    cur_db_connection = db_connect.cursor()
    for item in media_items:
        values = (item['id'], item['filename'], item['mimeType'], item['mediaMetadata']['creationTime'])
        try:
            cur_db_connection.execute('INSERT INTO my_media (object_id, filename, media_type, creation_time) \
            VALUES (?, ?, ?, ?)', values)
        except sqlite3.IntegrityError:
            logging.info('List has been retrieved.')
            return 10, new_next_page_token
        db_connect.commit()
    return 0, new_next_page_token


def get_media_files() -> int:
    """
    Downloads media files to media folder and marks 1 in 'stored' field.
    If file already exist, marks it 2 in 'stored' field.
    :return:
        0 - success.
        1 or 2 or 3 - unexpected error.
        4 - http 401 code, the token may have expired.
    """
    cur_db_connection = db_connect.cursor()
    cur_db_connection.execute("SELECT object_id, filename, media_type, creation_time FROM my_media WHERE stored = '0'")
    selection = cur_db_connection.fetchall()
    headers = {'Accept': 'application/json',
               'Authorization': 'Bearer ' + credentials['access_token']}
    params = {'key': API_KEY}

    for item in selection:
        r = requests.get(SRV_ENDPOINT+'mediaItems/'+item[0], params=params, headers=headers)
        if r.status_code == 401:
            return 4
        base_url = json.loads(r.text)['baseUrl']
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
            if os.path.exists(PATH_TO_IMAGES_STORAGE+item[1]):
                logging.warning(f"File {item[1]} already exist in local storage! Setting 'stored = 2' in database.")
                cur_db_connection.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (item[0],))
                db_connect.commit()
                continue
            if not os.path.exists(PATH_TO_IMAGES_STORAGE+subfolder_name):
                os.makedirs(PATH_TO_IMAGES_STORAGE+subfolder_name)
            f = open(PATH_TO_IMAGES_STORAGE+subfolder_name+item[1], 'wb')
            f.write(r.content)
            f.close()
        elif 'video' in r.headers['Content-Type']:
            if os.path.exists(PATH_TO_VIDEOS_STORAGE+item[1]):
                logging.warning(f"File {item[1]} already exist in local storage! Setting 'stored = 2' in database.")
                cur_db_connection.execute("UPDATE my_media SET stored='2' WHERE object_id=?", (item[0],))
                db_connect.commit()
                continue
            if not os.path.exists(PATH_TO_VIDEOS_STORAGE+subfolder_name):
                os.makedirs(PATH_TO_VIDEOS_STORAGE+subfolder_name)
            f = open(PATH_TO_VIDEOS_STORAGE+subfolder_name+item[1], 'wb')
            f.write(r.content)
            f.close()
        else:
            logging.error('Unexpected error.')
            return 3
        logging.info(f'Item {item[1]} stored.')
        cur_db_connection.execute("UPDATE my_media SET stored='1' WHERE object_id=?", (item[0],))
        db_connect.commit()
    return 0


def list_albums():  # TODO
    pass


def create_album(album_name):  # TODO
    pass


def add_to_album(album_id, item_id):  # TODO
    pass


def share_album(album_id):  # TODO
    pass


def create_subfolders_in_storage():  # TODO
    pass


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

get_auth()


logging.basicConfig(format='%(asctime)s %(levelname)s %(funcName)s: %(message)s',
                    filename=LOG_FILE_PATH, filemode='a', level=logging.INFO)
logging.info('Started.')

credentials = read_credentials()
db_connect = sqlite3.connect(DB_FILE_PATH)
logging.info('Start retrieving a list of media items.')

# Get list of media and write info into the DB.
result = (0, None)
while True:
    result = get_list_one_page(result[1])
    time.sleep(5)
    if result[0] == 30:
        refr_token()
        credentials = read_credentials()
    elif result[0] == 10:
        break
    elif result[0] != 0:
        logging.error(f"Application error. Returns code - {result[0]}.")
        db_connect.close()
        exit(1)

# Download media files to media folder.
logging.info('Start downloading a list of media items.')
while True:
    result = get_media_files()
    if result == 4:
        refr_token()
        credentials = read_credentials()
    else:
        break
db_connect.close()
logging.info('Finished.')
