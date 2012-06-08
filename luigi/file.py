import os, random

class atomic_file(file):
    # Simple class that writes to a temp file and moves it on close()
    # Also cleans up the temp file if close is not invoked
    def __init__(self, path):
        self.__tmp_path = path + '-luigi-tmp-%09d' % random.randrange(0, 999999999)
        self.__path = path
        super(atomic_file, self).__init__(self.__tmp_path, 'w')

    def close(self):
        super(atomic_file, self).close()
        os.rename(self.__tmp_path, self.__path)

    def __del__(self):
        if os.path.exists(self.__tmp_path):
            os.remove(self.__tmp_path)

    @property
    def tmp_path(self):
        return self.__tmp_path

class File(object):
    def __init__(self, path):
        self.__path = path

    def exists(self):
        return os.path.exists(self.__path)

    def open(self, mode='r'):
        if mode == 'w':
            # Create folder if it does not exist
            normpath = os.path.normpath(self.__path)
            parentfolder = os.path.dirname(normpath)
            if not os.path.exists(parentfolder):
                os.makedirs(parentfolder)

            return atomic_file(self.__path)

        elif mode == 'r':
            return open(self.__path, mode)
        else:
            raise Exception('mode must be r/w')
    
    def remove(self):
    	os.remove(self.__path)

    @property
    def fn(self):
        return self.__path
