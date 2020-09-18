#!/usr/bin/env python3.7

import os, urllib.request, urllib.parse, json, fileinput, sqlite3
from googleapiclient import discovery
from httplib2 import Http
from oauth2client import file, client, tools

## Define vars and checks
srv_endpoint = 'https://photoslibrary.googleapis.com/v1/'
api_key = 'AIzaSyCk1qpI9w87PqlS1SgJlwdroAGYqHgZEEs'
oauth2_file_path = 'storage-photos.json'
identity_file_pth = 'client_id.json'
dbconn = sqlite3.connect('db.sqlite')
c = dbconn.cursor()
scopes = 'https://www.googleapis.com/auth/photoslibrary'

## AUTH
store = file.Storage(oauth2_file_path)
creds = store.get()
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets(identity_file_pth, scopes)
    creds = tools.run_flow(flow, store)

## Checks
if not 'api_key' in globals():
    print(f"api_key - does not exits!")
    exit()
if not os.path.exists(oauth2_file_path):
    print(f"File {oauth2_file_path} does not exist!")
    exit()
with open(oauth2_file_path) as oauth2_file:
    oauth2_file_data = json.load(oauth2_file)
    access_token = oauth2_file_data['access_token']
    client_id = oauth2_file_data['client_id']
    client_secret = oauth2_file_data['client_secret']
    refresh_token = oauth2_file_data['refresh_token']

#token = {'Bearer ' access_token}
#print(access_token)



def refr_token() -> str:
    url = 'https://accounts.google.com/o/oauth2/token'
    values = {'client_id': client_id,
              'client_secret': client_secret,
              'refresh_token': refresh_token,
              'grant_type': 'refresh_token'}
    data = urllib.parse.urlencode(values)
    #print(data); exit()
    data = data.encode('ascii')
    req = urllib.request.Request(url, data)
    with urllib.request.urlopen(req) as response:
        the_page = response.read()
        #print(the_page)
        new_access_token = json.loads(the_page)['access_token']
        #print(f"New access_token {new_access_token} has been received.")
    for line in fileinput.input(oauth2_file_path, inplace=True):
        print (line.replace(access_token, new_access_token)),
    return new_access_token


#refr_token()

def list_media_obj():
    next_page_token = ''
    headers = {'Accept': 'application/json',
               'Authorization': 'Bearer ' + access_token,
               'key': api_key, 
               'pageSize': '10',
               'pageToken': next_page_token}
    #print(headers); exit()
    #req = urllib.request.Request(srv_endpoint, data=None, headers)
    #with urllib.request.urlopen(req) as response:
        #json_resp = response.read()
    #print(json_resp)
    urllib.request.urlopen({srv_endpoint}, data=bytes(json.dumps(headers)), encoding="utf-8")


list_media_obj()
