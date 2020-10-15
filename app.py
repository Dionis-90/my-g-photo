#!/usr/bin/env python3.7

import fileinput
import json
import os
import requests
import sqlite3
import time
from oauth2client import file, client, tools

# Define global vars
srv_endpoint = 'https://photoslibrary.googleapis.com/v1/'
api_key = 'AIzaSyCk1qpI9w87PqlS1SgJlwdroAGYqHgZEEs'
oauth2_file_path = 'storage-photos.json'
identity_file_pth = 'client_id.json'
dbconn = sqlite3.connect('db.sqlite')
scopes = 'https://www.googleapis.com/auth/photoslibrary'

# AUTH
store = file.Storage(oauth2_file_path)
creds = store.get()
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets(identity_file_pth, scopes)
    creds = tools.run_flow(flow, store)

# Checks
if not os.path.exists(oauth2_file_path):
    print(f"File {oauth2_file_path} does not exist!")
    exit()
def read_creds():
    global access_token
    global client_id
    global access_token
    global client_secret
    global refresh_token
    with open(oauth2_file_path) as oauth2_file:
        oauth2_file_data = json.load(oauth2_file)
        access_token = oauth2_file_data['access_token']
        client_id = oauth2_file_data['client_id']
        client_secret = oauth2_file_data['client_secret']
        refresh_token = oauth2_file_data['refresh_token']


read_creds()

def refr_token():
    url = 'https://accounts.google.com/o/oauth2/token'
    values = {'client_id': client_id,
              'client_secret': client_secret,
              'refresh_token': refresh_token,
              'grant_type': 'refresh_token'}
    r = requests.post(url, data = values)
    the_page = r.text
    new_access_token = json.loads(the_page)['access_token']
    for line in fileinput.input(oauth2_file_path, inplace=True):
        print (line.replace(access_token, new_access_token)),
    return new_access_token


def list_media_obj(next_page_token):
    url = srv_endpoint+'mediaItems'
    headers = {'Accept': 'application/json',
               'Authorization': 'Bearer ' + access_token}
    params = {'key': api_key,
              'pageSize': '10',
              'pageToken': next_page_token}
    r = requests.get(url, params=params, headers=headers)
    if r.status_code == 401:
        print('Refreshing token.')
        refr_token()
        read_creds()
        return 30
    the_page = r.text
    new_next_page_token = json.loads(the_page)['nextPageToken']
    media_items = json.loads(the_page)['mediaItems']
    c = dbconn.cursor()
    for item in media_items:
        filename = item['filename']
        values = (item['id'], item['filename'], item['mimeType'])
        try:
            c.execute('INSERT INTO my_media (object_id, filename, media_type) VALUES (?, ?, ?)', values)
        except sqlite3.IntegrityError:
            print(f'{filename} already exist.')
            return 10
        except Exception:
            print('Unexpected error.')
            return 20
        dbconn.commit()
    return new_next_page_token


list_media_obj_result = list_media_obj('')
if list_media_obj_result == 30:
    list_media_obj_result = list_media_obj('')
while type(list_media_obj_result) == str:
    list_media_obj_result = list_media_obj(list_media_obj_result)
    time.sleep(2)
dbconn.close()

