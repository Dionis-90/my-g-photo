import sqlite3
import requests
import logging

from app.tools import exceptions


def db_connect(db_file_path) -> sqlite3.Connection:
    db_logger = logging.getLogger('DB connection')
    try:
        db_conn = sqlite3.connect(db_file_path)
    except Exception as err:
        message = f'Fail to connect to DB {db_file_path}.\n{err}'
        print(message)
        db_logger.error(message)
        exit(10)
    return db_conn


def db_conn_pool(db_file_path, count=1) -> list:
    db_conn = []
    for _ in range(count):
        db_conn += db_connect(db_file_path)
    return db_conn


def make_request_w_auth(access_token, url, params=None):
    headers = {'Accept': 'application/json',
               'Authorization': 'Bearer ' + access_token}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 401:
        raise exceptions.SessionNotAuth('Session unauthorized.')
    elif response.status_code == 404:
        raise FileNotFoundError()
    elif response.status_code != 200:
        raise exceptions.MyGPhotoException(f'Response code: {response.status_code}. Response: {response.text}')
    try:
        representation = response.json()
    except ValueError:
        logging.exception('Response does not contain a json.')
        raise
    return representation
