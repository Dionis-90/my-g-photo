import logging


class MyBaseException(Exception):
    def __init__(self, *args):
        self.logger = logging.getLogger(self.__class__.__name__)
        if args:
            self.message = args[0]
        else:
            self.message = f'{self.__class__.__name__} has been raised.'


class ObjAlreadyExists(MyBaseException):
    def __init__(self, *args):
        super().__init__(*args)
        self.logger.info(self.message)


class SessionNotAuth(MyBaseException):
    def __init__(self, *args):
        super().__init__(*args)
        self.logger.error(self.message)


class DownloadError(MyBaseException):
    def __init__(self, *args):
        super().__init__(*args)
        self.logger.warning(self.message)


class NoItemsInResp(MyBaseException):
    def __init__(self, *args):
        super().__init__(*args)
        self.logger.info(self.message)


class NoNextPageTokenInResp(MyBaseException):
    def __init__(self, *args):
        super().__init__(*args)
        self.logger.warning(self.message)


class FailGettingPage(MyBaseException):
    def __init__(self, *args):
        super().__init__(*args)
        self.logger.exception(self.message)


class VideoNotReady(MyBaseException):
    def __init__(self, *args):
        super().__init__(*args)
        self.logger.warning(self.message)
