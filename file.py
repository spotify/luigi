import os

class File(object):
    def __init__(self, path):
        self.__path = path

    def exists(self):
        return os.path.exists(self.__path)

    def open(self, mode):
        return open(self.__path, mode)
