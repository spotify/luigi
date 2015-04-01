# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
This library is a wrapper of ftplib.
It is convenient to move data from/to FTP.

There is an example on how to use it (example/ftp_experiment_outputs.py)

You can also find unittest for each class.

Be aware that normal ftp do not provide secure communication.
"""

import datetime
import ftplib
import os
import sys
import random
import io

import luigi
import luigi.file
import luigi.format
import luigi.target
from luigi.format import FileWrapper, MixedUnicodeBytes


class RemoteFileSystem(luigi.target.FileSystem):

    def __init__(self, host, username=None, password=None, port=21, tls=False):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.tls = tls

    def _connect(self):
        """
        Log in to ftp.
        """
        if self.tls:
            self.ftpcon = ftplib.FTP_TLS()
        else:
            self.ftpcon = ftplib.FTP()
        self.ftpcon.connect(self.host, self.port)
        self.ftpcon.login(self.username, self.password)
        if self.tls:
            self.ftpcon.prot_p()

    def exists(self, path, mtime=None):
        """
        Return `True` if file or directory at `path` exist, False otherwise.

        Additional check on modified time when mtime is passed in.

        Return False if the file's modified time is older mtime.
        """
        self._connect()
        files = self.ftpcon.nlst(path)

        result = False
        if files:
            if mtime:
                mdtm = self.ftpcon.sendcmd('MDTM ' + path)
                modified = datetime.datetime.strptime(mdtm[4:], "%Y%m%d%H%M%S")
                result = modified > mtime
            else:
                result = True

        self.ftpcon.quit()

        return result

    def _rm_recursive(self, ftp, path):
        """
        Recursively delete a directory tree on a remote server.

        Source: https://gist.github.com/artlogic/2632647
        """
        wd = ftp.pwd()

        # check if it is a file first, because some FTP servers don't return
        # correctly on ftp.nlst(file)
        try:
            ftp.cwd(path)
        except ftplib.all_errors:
            # this is a file, we will just delete the file
            ftp.delete(path)
            return

        try:
            names = ftp.nlst()
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
        """
        Remove file or directory at location ``path``.

        :param path: a path within the FileSystem to remove.
        :type path: str
        :param recursive: if the path is a directory, recursively remove the directory and
                          all of its descendants. Defaults to ``True``.
        :type recursive: bool
        """
        self._connect()

        if recursive:
            self._rm_recursive(self.ftpcon, path)
        else:
            try:
                # try delete file
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
        self.ftpcon.retrbinary('RETR %s' % path, open(tmp_local_path, 'wb').write)
        self.ftpcon.quit()

        os.rename(tmp_local_path, local_path)


class AtomicFtpFile(luigi.target.AtomicLocalFile):
    """
    Simple class that writes to a temp file and upload to ftp on close().

    Also cleans up the temp file if close is not invoked.
    """

    def __init__(self, fs, path):
        """
        Initializes an AtomicFtpfile instance.
        :param fs:
        :param path:
        :type path: str
        """
        self._fs = fs
        super(AtomicFtpFile, self).__init__(path)

    def move_to_final_destination(self):
        self._fs.put(self.tmp_path, self.path)

    @property
    def fs(self):
        return self._fs


class RemoteTarget(luigi.target.FileSystemTarget):
    """
    Target used for reading from remote files.

    The target is implemented using ssh commands streaming data over the network.
    """

    def __init__(
        self, path, host, format=None, username=None,
        password=None, port=21, mtime=None, tls=False
    ):
        if format is None:
            format = luigi.format.get_default_format()

        # Allow to write unicode in file for retrocompatibility
        if sys.version_info[:2] <= (2, 6):
            format = format >> MixedUnicodeBytes

        self.path = path
        self.mtime = mtime
        self.format = format
        self.tls = tls
        self._fs = RemoteFileSystem(host, username, password, port, tls)

    @property
    def fs(self):
        return self._fs

    def open(self, mode):
        """
        Open the FileSystem target.

        This method returns a file-like object which can either be read from or written to depending
        on the specified mode.

        :param mode: the mode `r` opens the FileSystemTarget in read-only mode, whereas `w` will
                     open the FileSystemTarget in write mode. Subclasses can implement
                     additional options.
        :type mode: str
        """
        if mode == 'w':
            return self.format.pipe_writer(AtomicFtpFile(self._fs, self.path))

        elif mode == 'r':
            self.__tmp_path = self.path + '-luigi-tmp-%09d' % random.randrange(0, 1e10)
            # download file to local
            self._fs.get(self.path, self.__tmp_path)

            return self.format.pipe_reader(
                FileWrapper(io.BufferedReader(io.FileIO(self.__tmp_path, 'r')))
            )
        else:
            raise Exception('mode must be r/w')

    def exists(self):
        return self.fs.exists(self.path, self.mtime)

    def put(self, local_path):
        self.fs.put(local_path, self.path)

    def get(self, local_path):
        self.fs.get(self.path, local_path)
