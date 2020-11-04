#!/usr/bin/env python3.7

import fileinput
import json
import os
import requests
import sqlite3
import time
from oauth2client import file, client, tools

# Define constants
SRV_ENDPOINT = 'https://photoslibrary.googleapis.com/v1/'
API_KEY = 'AIzaSyCk1qpI9w87PqlS1SgJlwdroAGYqHgZEEs'
OAUTH2_FILE_PATH = 'storage-photos.json'
IDENTITY_FILE_PATH = 'client_id.json'
DB_FILE_PATH = 'db.sqlite'
SCOPES = 'https://www.googleapis.com/auth/photoslibrary'

if not os.path.exists(IDENTITY_FILE_PATH):
    print(f"File {IDENTITY_FILE_PATH} does not exist! Please put the file in working directory.")
    exit(1)

# AUTH
store = file.Storage(OAUTH2_FILE_PATH)
creds = store.get()
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets(IDENTITY_FILE_PATH, SCOPES)
    creds = tools.run_flow(flow, store)
if not os.path.exists(OAUTH2_FILE_PATH):
    print(f"File {OAUTH2_FILE_PATH} does not exist! Authentication unsuccessful.")
    exit(1)


def read_credentials():
    with open(OAUTH2_FILE_PATH) as oauth2_file:
        oauth2_file_data = json.load(oauth2_file)
        return oauth2_file_data


credentials = read_credentials()


def refr_token():
    values = {'client_id': credentials['client_id'],
              'client_secret': credentials['client_secret'],
              'refresh_token': credentials['refresh_token'],
              'grant_type': 'refresh_token'}
    response = requests.post(credentials['token_uri'], data=values)
    the_page = response.text
    new_access_token = json.loads(the_page)['access_token']
    for line in fileinput.input(OAUTH2_FILE_PATH, inplace=True):
        print(line.replace(credentials['access_token'], new_access_token)),
    return new_access_token

# TODO
"""
baseUrl make is temporary url, please do not store it
"""

def list_media_obj(next_page_token):
    """
    Gets list of media objects and put metadata to database.
    :param next_page_token: We receive this token in response after successful execution of this function.
        At the first run we need to set an empty string.
    :return: next_page_token or exit codes:
        10 - media object metadata already exists in database.
        20 - unknown error.
        30 - token expired and has been refreshed.
    """
    url = SRV_ENDPOINT+'mediaItems'
    headers = {'Accept': 'application/json',
               'Authorization': 'Bearer ' + credentials['access_token']}
    params = {'key': API_KEY,
              'pageSize': '10',
              'pageToken': next_page_token}
    r = requests.get(url, params=params, headers=headers)
    if r.status_code == 401:
        print('Refreshing token.')
        refr_token()
        return 30
    the_page = r.text
    new_next_page_token = json.loads(the_page)['nextPageToken']
    media_items = json.loads(the_page)['mediaItems']
    cur_db_connection = db_connect.cursor()
    for item in media_items:
        filename = item['filename']
        values = (item['id'], item['filename'], item['mimeType'], item['baseUrl'])
        try:
            cur_db_connection.execute('INSERT INTO my_media (object_id, filename, media_type, baseurl) \
            VALUES (?, ?, ?, ?)', values)
        except sqlite3.IntegrityError:
            print(f'{filename} already exist.')
            return 10
        except Exception:
            print('Unexpected error.')
            return 20
        db_connect.commit()
    return new_next_page_token


def get_media_files():
    cur_db_connection = db_connect.cursor()
    cur_db_connection.execute("SELECT baseurl, filename FROM my_media WHERE loaded != '1'")
    selection = cur_db_connection.fetchall()
    for item in selection:
        r = requests.get(item[0]+'=d', params=None, headers=None)
        binary_data = r.content
        f = open('media/'+item[1], 'wb')
        f.write(binary_data)
        f.close()
        cur_db_connection.execute("UPDATE my_media SET loaded='1' WHERE baseurl=?", (item[0],))
        db_connect.commit()
        time.sleep(2)
    return 0


# Get list of media and write info into the DB.
db_connect = sqlite3.connect(DB_FILE_PATH)
list_media_obj_result = list_media_obj('')
if list_media_obj_result == 30:
    credentials = read_credentials()
    list_media_obj_result = list_media_obj('')
while type(list_media_obj_result) == str:
    list_media_obj_result = list_media_obj(list_media_obj_result)
    time.sleep(2)
get_media_files()
db_connect.close()
