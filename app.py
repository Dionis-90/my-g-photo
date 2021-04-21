#!/usr/bin/env python3.8
# This is an application that gets, downloads media files and metadata from your Google Photo storage to your
# local storage.

import shutil
from lib import *


class Metadata:
    def __init__(self):
        self.new_next_page_token = None
        self.current_mode = '0'
        self.list_retrieved = False
        self.logger = logging.getLogger(self.__class__.__name__)
        self.db_conn = None

    def __get_page(self, auth, next_page_token) -> json:
        url = SRV_ENDPOINT + 'mediaItems'
        objects_count_on_page = '100'
        params = {'pageSize': objects_count_on_page,
                  'pageToken': next_page_token}
        headers = {'Accept': 'application/json',
                   'Authorization': 'Bearer ' + auth.access_token}
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 401:
            raise SessionNotAuth("Unauthorized.")
        elif response.status_code != 200:
            raise FailGettingPage(f"http code {response.status_code} when trying to get page of list with "
                                  f"next_page_token: {next_page_token}, response: {response.text}")
        try:
            page = response.json()['mediaItems']
        except KeyError:
            raise NoItemsInResp(f"No mediaItems object in response. Response: {response.text}")
        try:
            self.new_next_page_token = response.json()['nextPageToken']
        except KeyError:
            self.new_next_page_token = None
            raise NoNextPageTokenInResp("No nextPageToken object in response. Probably got end of the list.")
        return page

    @staticmethod
    def __write_page(page, mode='write_all'):
        """
        :param mode: 'write_all' or 'write_latest'
        """
        for item in page:
            media_item = MediaItem(item['id'], item['mimeType'], item['filename'],
                                   item['mediaMetadata']['creationTime'])
            try:
                media_item.write_to_db()
            except ObjAlreadyExists:
                if mode == 'write_all':
                    continue
                elif mode == 'write_latest':
                    raise
                else:
                    raise Exception('Unexpected error.')

    def __check_mode(self):
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("SELECT value FROM account_info WHERE key='list_received'")
        except sqlite3.Error as err:
            self.logger.error(f'DB query failed, {err}.')
            raise
        try:
            self.current_mode = cursor.fetchone()[0]
        except TypeError:
            pass

    def get_metadata_list(self, auth):
        """Gets media metadata from Google Photo server and writes it to the local database."""
        self.db_conn = db_connect()
        self.__check_mode()
        cursor = self.db_conn.cursor()
        self.logger.info(f'Running in mode {self.current_mode}.')
        pages = 0
        while True:
            try:
                page = self.__get_page(auth, self.new_next_page_token)
            except SessionNotAuth:
                auth.refresh_access_token()
                continue
            except (NoNextPageTokenInResp, NoItemsInResp):
                self.list_retrieved = True
            except FailGettingPage:
                break
            if self.current_mode == '0':
                self.__write_page(page)
            elif self.current_mode == '1':
                try:
                    self.__write_page(page, mode='write_latest')
                except ObjAlreadyExists:
                    break
            else:
                raise Exception('Unexpected error.')
            pages += 1
            self.logger.info(f'{pages} - processed.')
            if self.list_retrieved:
                cursor.execute("UPDATE account_info SET value='1' WHERE key='list_received'")
                self.db_conn.commit()
                self.logger.warning("List has been retrieved.")
                break


class LocalStorage:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.db_conn = None
        self.download_selection = None
        self.actualization_selection = None
        self.last_actualization_date = None

    def __get_download_selection(self):
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT object_id, media_type, filename, creation_time FROM my_media "
                           "WHERE stored = '0' ORDER BY creation_time DESC")
        except sqlite3.Error as err:
            self.logger.error(f'Fail to communicate with DB.\n{err}')
            raise
        self.download_selection = cursor.fetchall()

    def __create_tree(self):
        sub_folders = set()
        for item in self.download_selection:
            year = datetime.datetime.strptime(item[3], "%Y-%m-%dT%H:%M:%SZ").year
            sub_folders.add(str(year))
        for item in sub_folders:
            if not os.path.exists(PATH_TO_IMAGES_STORAGE + item):
                try:
                    os.makedirs(PATH_TO_IMAGES_STORAGE + item)
                except OSError as err:
                    self.logger.error(f"Fail to create folder, {err}")
                    raise
                self.logger.info(f"Folder {PATH_TO_IMAGES_STORAGE + item} has been created.")
            if not os.path.exists(PATH_TO_VIDEOS_STORAGE + item):
                try:
                    os.makedirs(PATH_TO_VIDEOS_STORAGE + item)
                except OSError as err:
                    self.logger.error(f"Fail to create folder, {err}")
                    raise
                self.logger.info(f"Folder {PATH_TO_VIDEOS_STORAGE + item} has been created.")

    def __get_actualization_selection(self):
        cursor = self.db_conn.cursor()
        cursor.execute("SELECT value FROM account_info WHERE key = 'last_processed_object_id'")
        last_local_id_processed = cursor.fetchall()
        if not last_local_id_processed:
            cursor.execute("SELECT object_id, media_type, filename, creation_time FROM my_media "
                           "WHERE stored != '0' ORDER BY id")
        else:
            cursor.execute("SELECT object_id, media_type, filename, creation_time FROM my_media WHERE \
                                   stored != '0' and id > (SELECT id FROM my_media WHERE object_id = ?) ORDER BY id",
                           last_local_id_processed[0])
        self.actualization_selection = cursor.fetchall()

    def __get_last_actualization(self):
        cursor = self.db_conn.cursor()
        cursor.execute("DELETE from account_info WHERE key = 'last_processed_object_id'")
        self.db_conn.commit()
        cursor.execute("SELECT value FROM account_info WHERE key = 'last_actualization'")
        try:
            self.last_actualization_date = cursor.fetchone()[0]
        except TypeError:
            pass

    def __is_actualization_needed(self) -> bool:
        if not self.last_actualization_date:
            return True
        difference = datetime.datetime.now() - \
            datetime.datetime.strptime(self.last_actualization_date, "%Y-%m-%dT%H:%M:%SZ")
        difference = difference.days
        if difference < ACTUALIZATION_PERIOD:
            return False
        return True

    def __set_last_actualization_date(self):
        cursor = self.db_conn.cursor()
        now = datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%SZ")
        cursor.execute(f"INSERT OR REPLACE INTO account_info (key, value) VALUES ('last_actualization', '{now}')")
        self.db_conn.commit()

    def find_and_clean_not_existing(self, auth) -> bool:
        if not self.__is_actualization_needed():
            return False
        self.logger.info("Start local DB and storage actualization.")
        self.db_conn = db_connect()
        self.__get_last_actualization()
        self.__get_actualization_selection()
        cursor = self.db_conn.cursor()
        for item in self.actualization_selection:
            media_item = MediaItem(*item)
            try:
                result = media_item.is_exist_on_server(auth)
            except SessionNotAuth:
                auth.refresh_access_token()
                try:
                    result = media_item.is_exist_on_server(auth)
                except (Exception, KeyboardInterrupt):
                    cursor.execute("INSERT INTO account_info (key, value) "
                                   "VALUES('last_processed_object_id', ?)", (media_item.id,))
                    self.db_conn.commit()
                    raise
            except (Exception, KeyboardInterrupt):
                cursor.execute("INSERT INTO account_info (key, value) "
                               "VALUES('last_processed_object_id', ?)", (media_item.id,))
                self.db_conn.commit()
                raise
            if not result:
                media_item.remove_from_local()
                media_item.remove_from_db()
                self.logger.info(f"{media_item.filename} removed from local and db.")
        self.__set_last_actualization_date()
        return True

    def get_media_items(self, auth):
        """Downloads media items that listed in the database putting it by years folder."""
        self.db_conn = db_connect()
        self.__get_download_selection()
        try:
            self.__create_tree()
        except OSError:
            self.logger.error("Please check storage paths in config.")
            raise
        for item in self.download_selection:
            media_item = MediaItem(*item)
            try:
                media_item.get_base_url(auth)
            except FileNotFoundError:
                self.logger.warning(f"Item {item[2]} not found on the server, removing from database.")
                media_item.remove_from_db()
                continue
            except SessionNotAuth:
                auth.refresh_access_token()
                media_item.get_base_url(auth)
            except Exception:
                self.logger.error(f'Fail to update base url by {item[2]}.')
                raise
            try:
                media_item.download()
            except (FileExistsError, OSError):
                continue
        self.logger.info('Getting media items is complete.')


class Runtime:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info('Started.')
        self.authentication = Authentication()
        self.metadata = Metadata()
        self.local_storage = LocalStorage()

    def __is_db_exists(self) -> bool:
        if not os.path.exists(DB_FILE_PATH):
            message = f'DB {DB_FILE_PATH} does not exist.'
            print(message)
            self.logger.error(message)
            return False
        return True

    def __db_creation(self) -> bool:
        answer = input('Do you want to create new DB?(Y/n)')
        if answer == 'n' or answer == 'N':
            self.logger.warning('Aborted by user.')
            return False
        shutil.copy(DB_FILE_PATH + '.structure', DB_FILE_PATH)
        return True

    def main(self):
        try:
            self.authentication.authenticate()
        except Exception as err:
            self.logger.error(f"Fail to authenticate.\n{err}")
            exit(5)
        if not self.__is_db_exists():
            if not self.__db_creation():
                exit(0)
        try:
            self.metadata.get_metadata_list(self.authentication)
        except Exception as err:
            self.logger.error(f"Unexpected error.\n{err}")
            exit(1)
        except KeyboardInterrupt:
            self.logger.warning("Aborted by user.")
            exit(0)
        self.logger.info('Start downloading a list of media items.')
        try:
            self.local_storage.get_media_items(self.authentication)
        except Exception as err:
            self.logger.error(f"Fail to download media: {err}")
            exit(3)
        except KeyboardInterrupt:
            self.logger.warning("Aborted by user.")
            exit(0)
        try:
            self.local_storage.find_and_clean_not_existing(self.authentication)
        except Exception as err:
            self.logger.error(f'Fail to actualize DB.\n{err}')
            exit(8)
        except KeyboardInterrupt:
            self.logger.warning("Aborted by user.")
            exit(0)
        self.logger.info('Actualization is complete.')
        self.logger.info('Finished.')


if __name__ == '__main__':
    Runtime().main()
