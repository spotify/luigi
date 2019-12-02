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

# this is an integration test. to run this test requires that an actuall FTP server
# is running somewhere. to run a local ftp server do the following
# pip install pyftpdlib==1.5.0
# mkdir /tmp/luigi-test-ftp/
# sudo python -m _test_ftp


import datetime
import ftplib
import os
import shutil
import sys
from helpers import unittest

from io import StringIO


from luigi.contrib.ftp import RemoteFileSystem, RemoteTarget

# dumb files
FILE1 = """this is file1"""
FILE2 = """this is file2"""
FILE3 = """this is file3"""

HOST = "localhost"
USER = "luigi"
PWD = "some_password"


class TestFTPFilesystem(unittest.TestCase):

    def setUp(self):
        """ Creates structure

        /test
        /test/file1
        /test/hola/
        /test/hola/file2
        /test/hola/singlefile
        /test/hola/file3
        """
        # create structure
        ftp = ftplib.FTP(HOST, USER, PWD)
        ftp.cwd('/')
        ftp.mkd('test')
        ftp.cwd('test')
        ftp.mkd('hola')
        ftp.cwd('hola')
        f2 = StringIO(FILE2)
        ftp.storbinary('STOR file2', f2)     # send the file
        f3 = StringIO(FILE3)
        ftp.storbinary('STOR file3', f3)     # send the file
        ftp.cwd('..')
        f1 = StringIO(FILE1)
        ftp.storbinary('STOR file1', f1)     # send the file
        ftp.close()

    def test_file_remove(self):
        """ Delete with recursive deactivated """
        rfs = RemoteFileSystem(HOST, USER, PWD)
        rfs.remove('/test/hola/file3', recursive=False)
        rfs.remove('/test/hola/file2', recursive=False)
        rfs.remove('/test/hola', recursive=False)
        rfs.remove('/test/file1', recursive=False)
        rfs.remove('/test', recursive=False)

        ftp = ftplib.FTP(HOST, USER, PWD)
        list_dir = ftp.nlst()

        self.assertFalse("test" in list_dir)

    def test_recursive_remove(self):
        """ Test FTP filesystem removing files recursive """
        rfs = RemoteFileSystem(HOST, USER, PWD)
        rfs.remove('/test')

        ftp = ftplib.FTP(HOST, USER, PWD)
        list_dir = ftp.nlst()

        self.assertFalse("test" in list_dir)


class TestFTPFilesystemUpload(unittest.TestCase):

    def test_single(self):
        """ Test upload file with creation of intermediate folders """
        ftp_path = "/test/nest/luigi-test"
        local_filepath = "/tmp/luigi-test-ftp"

        # create local temp file
        with open(local_filepath, 'w') as outfile:
            outfile.write("something to fill")

        rfs = RemoteFileSystem(HOST, USER, PWD)
        rfs.put(local_filepath, ftp_path)

        # manually connect to ftp
        ftp = ftplib.FTP(HOST, USER, PWD)
        ftp.cwd("/test/nest")
        list_dir = ftp.nlst()
        # file is successfuly created
        self.assertTrue("luigi-test" in list_dir)

        # delete tmp files
        ftp.delete("luigi-test")
        ftp.cwd("/")
        ftp.rmd("/test/nest")
        ftp.rmd("test")
        os.remove(local_filepath)
        ftp.close()


class TestRemoteTarget(unittest.TestCase):

    def test_put(self):
        """ Test RemoteTarget put method with uploading to an FTP """
        local_filepath = "/tmp/luigi-remotetarget-write-test"
        remote_file = "/test/example.put.file"

        # create local temp file
        with open(local_filepath, 'w') as outfile:
            outfile.write("something to fill")

        remotetarget = RemoteTarget(remote_file, HOST, username=USER, password=PWD)
        remotetarget.put(local_filepath)

        # manually connect to ftp
        ftp = ftplib.FTP(HOST, USER, PWD)
        ftp.cwd("/test")
        list_dir = ftp.nlst()

        # file is successfuly created
        self.assertTrue(remote_file.split("/")[-1] in list_dir)

        # clean
        os.remove(local_filepath)
        ftp.delete(remote_file)
        ftp.cwd("/")
        ftp.rmd("test")
        ftp.close()

    def test_get(self):
        """ Test Remote target get method downloading a file from ftp """
        local_filepath = "/tmp/luigi-remotetarget-read-test"
        tmp_filepath = "/tmp/tmp-luigi-remotetarget-read-test"
        remote_file = "/test/example.get.file"

        # create local temp file
        with open(tmp_filepath, 'w') as outfile:
            outfile.write("something to fill")

        # manualy upload to ftp
        ftp = ftplib.FTP(HOST, USER, PWD)
        ftp.mkd("test")
        ftp.storbinary('STOR %s' % remote_file, open(tmp_filepath, 'rb'))
        ftp.close()

        # execute command
        remotetarget = RemoteTarget(remote_file, HOST, username=USER, password=PWD)
        remotetarget.get(local_filepath)

        # make sure that it can open file
        with remotetarget.open('r') as fin:
            self.assertEqual(fin.read(), "something to fill")

        # check for cleaning temporary files
        if sys.version_info >= (3, 2):
            # cleanup uses tempfile.TemporaryDirectory only available in 3.2+
            temppath = remotetarget._RemoteTarget__tmp_path
            self.assertTrue(os.path.exists(temppath))
            remotetarget = None  # garbage collect remotetarget
            self.assertFalse(os.path.exists(temppath))

        # file is successfuly created
        self.assertTrue(os.path.exists(local_filepath))

        # test RemoteTarget with mtime
        ts = datetime.datetime.now() - datetime.timedelta(days=2)
        delayed_remotetarget = RemoteTarget(remote_file, HOST, username=USER, password=PWD, mtime=ts)
        self.assertTrue(delayed_remotetarget.exists())

        ts = datetime.datetime.now() + datetime.timedelta(days=2)  # who knows what timezone it is in
        delayed_remotetarget = RemoteTarget(remote_file, HOST, username=USER, password=PWD, mtime=ts)
        self.assertFalse(delayed_remotetarget.exists())

        # clean
        os.remove(local_filepath)
        os.remove(tmp_filepath)
        ftp = ftplib.FTP(HOST, USER, PWD)
        ftp.delete(remote_file)
        ftp.cwd("/")
        ftp.rmd("test")
        ftp.close()


def _run_ftp_server():
    from pyftpdlib.authorizers import DummyAuthorizer
    from pyftpdlib.handlers import FTPHandler
    from pyftpdlib.servers import FTPServer

    # Instantiate a dummy authorizer for managing 'virtual' users
    authorizer = DummyAuthorizer()

    tmp_folder = '/tmp/luigi-test-ftp-server/'
    if os.path.exists(tmp_folder):
        shutil.rmtree(tmp_folder)
    os.mkdir(tmp_folder)

    authorizer.add_user(USER, PWD, tmp_folder, perm='elradfmwM')
    handler = FTPHandler
    handler.authorizer = authorizer
    address = ('localhost', 21)
    server = FTPServer(address, handler)
    server.serve_forever()


if __name__ == '__main__':
    _run_ftp_server()
