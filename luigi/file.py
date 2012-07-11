import os
import random


class atomic_file(file):
    # Simple class that writes to a temp file and moves it on close()
    # Also cleans up the temp file if close is not invoked
    def __init__(self, path):
        self.__tmp_path = path + '-luigi-tmp-%09d' % random.randrange(0, 999999999)
        self.path = path
        super(atomic_file, self).__init__(self.__tmp_path, 'w')

    def close(self):
        super(atomic_file, self).close()
        os.rename(self.__tmp_path, self.path)

    def __del__(self):
        if os.path.exists(self.__tmp_path):
            os.remove(self.__tmp_path)

    @property
    def tmp_path(self):
        return self.__tmp_path


class File(object):
    def __init__(self, path, format=None):
        self.path = path
        self.format = format

    def exists(self):
        return os.path.exists(self.path)

    def open(self, mode='r'):
        if mode == 'w':
            # Create folder if it does not exist
            normpath = os.path.normpath(self.path)
            parentfolder = os.path.dirname(normpath)
            if not os.path.exists(parentfolder):
                os.makedirs(parentfolder)

            if self.format:
                return self.format.pipe_writer(atomic_file(self.path))
            else:
                return atomic_file(self.path)

        elif mode == 'r':
            if self.format:
                return self.format.pipe_reader(file(self.path))
            else:
                return open(self.path, mode)
        else:
            raise Exception('mode must be r/w')

    def remove(self):
        os.remove(self.path)

    @property
    def fn(self):
        return self.path
