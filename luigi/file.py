import os

class File(object):
    def __init__(self, path):
        self.__path = path

    def exists(self):
        return os.path.exists(self.__path)

    def open(self, mode = 'r'):
        if mode == 'w' or mode == 'a':
            # Create folder if it does not exist
            normpath = os.path.normpath(self.__path)
            parentfolder = os.path.dirname(normpath)
            if not os.path.exists(parentfolder):
                os.makedirs(parentfolder)
        return open(self.__path, mode)
    
    def remove(self):
    	os.remove(self.__path)

    @property
    def fn(self):
        return self.__path
