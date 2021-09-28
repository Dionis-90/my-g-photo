import logging


class MyGPhotoException(Exception):
    def __init__(self, *args):
        self.logger = logging.getLogger(self.__class__.__name__)
        if args:
            self.message = args[0]
        else:
            self.message = f'{self.__class__.__name__} has been raised.'


class ObjAlreadyExists(MyGPhotoException):
    def __init__(self, *args):
        super().__init__(*args)
        self.logger.info(self.message)


class SessionNotAuth(MyGPhotoException):
    def __init__(self, *args):
        super().__init__(*args)
        self.logger.error(self.message)


class DownloadError(MyGPhotoException):
    def __init__(self, *args):
        super().__init__(*args)
        self.logger.warning(self.message)


class NoItemsInResp(MyGPhotoException):
    def __init__(self, *args):
        super().__init__(*args)
        self.logger.info(self.message)


class NoNextPageTokenInResp(MyGPhotoException):
    def __init__(self, *args):
        super().__init__(*args)
        self.logger.warning(self.message)


class FailGettingPage(MyGPhotoException):
    def __init__(self, *args):
        super().__init__(*args)
        self.logger.exception(self.message)


class VideoNotReady(MyGPhotoException):
    def __init__(self, *args):
        super().__init__(*args)
        self.logger.warning(self.message)
