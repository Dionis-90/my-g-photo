import logging

logger = logging.getLogger('Exceptions')


class ObjAlreadyExists(Exception):
    def __init__(self, message):
        logger.info(message)


class SessionNotAuth(Exception):
    def __init__(self, message):
        logger.warning(message)


class DownloadError(Exception):
    def __init__(self, message):
        logger.exception(message)


class NoItemsInResp(Exception):
    def __init__(self, message):
        logger.warning(message)


class NoNextPageTokenInResp(Exception):
    def __init__(self, message):
        logger.warning(message)


class FailGettingPage(Exception):
    def __init__(self, message):
        logger.exception(message)
