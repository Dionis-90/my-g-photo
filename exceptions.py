import logging


class MyExceptions(Exception):
    def __init__(self, *args):
        self.logger = logging.getLogger(self.__class__.__name__)
        if args:
            self.message = args[0]
        else:
            self.message = f'{self.__class__.__name__} has been raised.'
        self.logger.exception(self.message)


class ObjAlreadyExists(MyExceptions):
    pass


class SessionNotAuth(MyExceptions):
    pass


class DownloadError(MyExceptions):
    pass


class NoItemsInResp(MyExceptions):
    pass


class NoNextPageTokenInResp(MyExceptions):
    pass


class FailGettingPage(MyExceptions):
    pass


class VideoNotReady(MyExceptions):
    pass
