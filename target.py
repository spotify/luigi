from . import *

class Target(object):
    def __init__(self, file, task):
        self.__file = file
        self.__task = task

    def get_task(self):
        return self.__task

    def get_file(self):
        return self.__file

    def exists(self):
        return self.__file.exists()

    def open(self, mode):
        return self.__file.open(mode)

