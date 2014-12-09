"""
This library is a wrapper of ftplib. It is convenient to move data from/to FTP.

There is an example on how to use it (example/ftp_experiment_outputs.py)

You can also find unittest for each class.

Be aware that normal ftp do not provide secure communication.
"""
import os
import random
import ftplib
import luigi
import luigi.target
import luigi.format
from luigi.format import FileWrapper


class RemoteFileSystem(luigi.target.FileSystem):
    def __init__(self, host, username=None, password=None):
        self.host = host
        self.username = username
        self.password = password

    def _connect(self):
        """ Log in to ftp """
        self.ftpcon = ftplib.FTP(self.host, self.username, self.password)

    def exists(self, path):
        """ Return `True` if file or directory at `path` exist, False otherwise """
        self._connect()
        files = self.ftpcon.nlst(path)

        # empty list, means do not exists
        if not files:
            return False

        self.ftpcon.quit()

        return True

    def _rm_recursive(self, ftp, path):
        """
        Recursively delete a directory tree on a remote server.

        Source: https://gist.github.com/artlogic/2632647
        """
        wd = ftp.pwd()

        try:
            names = ftp.nlst(path)
        except ftplib.all_errors as e:
            # some FTP servers complain when you try and list non-existent paths
            return

        for name in names:
            if os.path.split(name)[1] in ('.', '..'):
                continue

            try:
                ftp.cwd(name)  # if we can cwd to it, it's a folder
                ftp.cwd(wd)  # don't try a nuke a folder we're in
                self._rm_recursive(ftp, name)
            except ftplib.all_errors:
                ftp.delete(name)

        try:
            ftp.rmd(path)
        except ftplib.all_errors as e:
            print('_rm_recursive: Could not remove {0}: {1}'.format(path, e))

    def remove(self, path, recursive=True):
        """ Remove file or directory at location ``path``

        :param str path: a path within the FileSystem to remove.
        :param bool recursive: if the path is a directory, recursively remove the directory and all
                               of its descendants. Defaults to ``True``.
        """
        self._connect()

        if recursive:
            self._rm_recursive(self.ftpcon, path)
        else:
            try:
                #try delete file
                self.ftpcon.delete(path)
            except ftplib.all_errors:
                # it is a folder, delete it
                self.ftpcon.rmd(path)

        self.ftpcon.quit()

    def put(self, local_path, path):
        # create parent folder if not exists
        self._connect()

        normpath = os.path.normpath(path)
        folder = os.path.dirname(normpath)

        # create paths if do not exists
        for subfolder in folder.split(os.sep):
            if subfolder and subfolder not in self.ftpcon.nlst():
                self.ftpcon.mkd(subfolder)

            self.ftpcon.cwd(subfolder)

        # go back to ftp root folder
        self.ftpcon.cwd("/")

        # random file name
        tmp_path = folder + os.sep + 'luigi-tmp-%09d' % random.randrange(0, 1e10)

        self.ftpcon.storbinary('STOR %s' % tmp_path, open(local_path, 'rb'))
        self.ftpcon.rename(tmp_path, normpath)

        self.ftpcon.quit()

    def get(self, path, local_path):
        # Create folder if it does not exist
        normpath = os.path.normpath(local_path)
        folder = os.path.dirname(normpath)
        if folder and not os.path.exists(folder):
            os.makedirs(folder)

        tmp_local_path = local_path + '-luigi-tmp-%09d' % random.randrange(0, 1e10)
        # download file
        self._connect()
        self.ftpcon.retrbinary('RETR %s' % path,  open(tmp_local_path, 'wb').write)
        self.ftpcon.quit()

        os.rename(tmp_local_path, local_path)


class AtomicFtpfile(file):
    """ Simple class that writes to a temp file and upload to ftp on close().
     Also cleans up the temp file if close is not invoked.
    """
    def __init__(self, fs, path):
        self.__tmp_path = self.path + '-luigi-tmp-%09d' % random.randrange(0, 1e10)
        self._fs = fs
        self.path = path
        super(AtomicFtpfile, self).__init__(self.__tmp_path, 'w')

    def close(self):
        # close and upload file to ftp
        super(AtomicFtpfile, self).close()
        self._fs.put(self.__tmp_path, self.path)
        os.remove(self.__tmp_path)

    def __del__(self):
        if os.path.exists(self.__tmp_path):
            os.remove(self.__tmp_path)

    @property
    def tmp_path(self):
        return self.__tmp_path

    @property
    def fs(self):
        return self._fs

    def __exit__(self, exc_type, exc, traceback):
        """
        Close/commit the file if there are no exception
        Upload file to ftp
        """
        if exc_type:
            return
        return file.__exit__(self, exc_type, exc, traceback)


class RemoteTarget(luigi.target.FileSystemTarget):
    """
    Target used for reading from remote files. The target is implemented using
    ssh commands streaming data over the network.
    """
    def __init__(self, path, host, format=None, username=None, password=None):
        self.path = path
        self.format = format
        self._fs = RemoteFileSystem(host, username, password)

    @property
    def fs(self):
        return self._fs

    def open(self, mode):
        """Open the FileSystem target.

        This method returns a file-like object which can either be read from or written to depending
        on the specified mode.

        :param str mode: the mode `r` opens the FileSystemTarget in read-only mode, whereas `w` will
                         open the FileSystemTarget in write mode. Subclasses can implement
                         additional options.
        """
        if mode == 'w':
            if self.format:
                return self.format.pipe_writer(AtomicFtpfile(self._fs, self.path))
            else:
                return AtomicFtpfile(self._fs, self.path)

        elif mode == 'r':
            self.__tmp_path = self.path + '-luigi-tmp-%09d' % random.randrange(0, 1e10)
            # download file to local
            self._fs.get(self.path, self.__tmp_path)

            # manage tmp file
            fileobj = FileWrapper(open(self.__tmp_path, 'r'))
            if self.format:
                return self.format.pipe_reader(fileobj)
            return fileobj
        else:
            raise Exception('mode must be r/w')

    def put(self, local_path):
        self.fs.put(local_path, self.path)

    def get(self, local_path):
        self.fs.get(self.path, local_path)
