import logging


class ObjAlreadyExists(Exception):
    def __init__(self, message):
        logging.info(message)


class SessionNotAuth(Exception):
    def __init__(self, message):
        logging.warning(message)


class DownloadError(Exception):
    def __init__(self, message):
        logging.error(message)


class NoItemsInResp(Exception):
    def __init__(self, message):
        logging.warning(message)


class NoNextPageTokenInResp(Exception):
    def __init__(self, message):
        logging.warning(message)


class FailGettingPage(Exception):
    def __init__(self, message):
        logging.error(message)
