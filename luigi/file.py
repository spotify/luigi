import os

class File(object):
    def __init__(self, path):
        self.__path = path

    def exists(self):
        return os.path.exists(self.__path)

    def open(self, mode = 'r'):
        return open(self.__path, mode)
    
    def remove(self):
    	os.remove(self.__path)

    @property
    def fn(self):
        return self.__path
