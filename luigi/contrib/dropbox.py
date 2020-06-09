# -*- coding: utf-8 -*-
#
# Copyright (c) 2019 Jose-Ignacio Riaño Chico
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

import logging
import ntpath
import os
import random
import tempfile
import time
from contextlib import contextmanager
from functools import wraps

import luigi.format
from luigi.target import FileSystem, FileSystemTarget, AtomicLocalFile

logger = logging.getLogger('luigi-interface')

try:
    import dropbox.dropbox
    import dropbox.exceptions
    import dropbox.files
except ImportError:
    logger.warning("Loading Dropbox module without the python package dropbox (https://pypi.org/project/dropbox/). "
                   "Will crash at runtime if Dropbox functionality is used.")


def accept_trailing_slash_in_existing_dirpaths(func):
    @wraps(func)
    def wrapped(self, path, *args, **kwargs):
        if path != '/' and path.endswith('/'):
            logger.warning("Dropbox paths should NOT have trailing slashes. This causes additional API calls")
            logger.warning("Consider modifying your calls to {}, so that they don't use paths than end with '/'".format(func.__name__))

            if self._exists_and_is_dir(path[:-1]):
                path = path[:-1]

        return func(self, path, *args, **kwargs)

    return wrapped


def accept_trailing_slash(func):
    @wraps(func)
    def wrapped(self, path, *args, **kwargs):
        if path != '/' and path.endswith('/'):
            path = path[:-1]
        return func(self, path, *args, **kwargs)

    return wrapped


class DropboxClient(FileSystem):
    """
    Dropbox client for authentication, designed to be used by the :py:class:`DropboxTarget` class.
    """

    def __init__(self, token, user_agent="Luigi"):
        """
        :param str token: Dropbox Oauth2 Token. See :class:`DropboxTarget` for more information about generating a token
        """
        if not token:
            raise ValueError("The token parameter must contain a valid Dropbox Oauth2 Token")

        try:
            conn = dropbox.dropbox.Dropbox(oauth2_access_token=token, user_agent=user_agent)
        except Exception as e:
            raise Exception("Cannot connect to Dropbox. Check your Internet connection and the token. \n" + repr(e))

        self.token = token
        self.conn = conn

    @accept_trailing_slash_in_existing_dirpaths
    def exists(self, path):
        if path == '/':
            return True
        if path.endswith('/'):
            path = path[:-1]
            return self._exists_and_is_dir(path)

        try:
            self.conn.files_get_metadata(path)
            return True
        except dropbox.exceptions.ApiError as e:
            if isinstance(e.error.get_path(), dropbox.files.LookupError):
                return False
            else:
                raise e

    @accept_trailing_slash_in_existing_dirpaths
    def remove(self, path, recursive=True, skip_trash=True):
        if not self.exists(path):
            return False
        self.conn.files_delete_v2(path)
        return True

    @accept_trailing_slash
    def mkdir(self, path, parents=True, raise_if_exists=False):
        if self.exists(path):
            if not self.isdir(path):
                raise luigi.target.NotADirectory()
            elif raise_if_exists:
                raise luigi.target.FileAlreadyExists()
            else:
                return

        self.conn.files_create_folder_v2(path)

    @accept_trailing_slash_in_existing_dirpaths
    def isdir(self, path):
        if path == '/':
            return True
        try:
            md = self.conn.files_get_metadata(path)
            return isinstance(md, dropbox.files.FolderMetadata)
        except dropbox.exceptions.ApiError as e:
            if isinstance(e.error.get_path(), dropbox.files.LookupError):
                return False
            else:
                raise e

    @accept_trailing_slash_in_existing_dirpaths
    def listdir(self, path, **kwargs):
        dirs = []
        lister = self.conn.files_list_folder(path, recursive=True, **kwargs)
        dirs.extend(lister.entries)
        while lister.has_more:
            lister = self.conn.files_list_folder_continue(lister.cursor)
            dirs.extend(lister.entries)
        return [d.path_display for d in dirs]

    @accept_trailing_slash_in_existing_dirpaths
    def move(self, path, dest):
        self.conn.files_move_v2(from_path=path, to_path=dest)

    @accept_trailing_slash_in_existing_dirpaths
    def copy(self, path, dest):
        self.conn.files_copy_v2(from_path=path, to_path=dest)

    def download_as_bytes(self, path):
        metadata, response = self.conn.files_download(path)
        return response.content

    def upload(self, tmp_path, dest_path):
        with open(tmp_path, 'rb') as f:
            file_size = os.path.getsize(tmp_path)

            CHUNK_SIZE = 4 * 1000 * 1000
            upload_session_start_result = self.conn.files_upload_session_start(f.read(CHUNK_SIZE))
            commit = dropbox.files.CommitInfo(path=dest_path)
            cursor = dropbox.files.UploadSessionCursor(session_id=upload_session_start_result.session_id,
                                                       offset=f.tell())

            if f.tell() >= file_size:
                self.conn.files_upload_session_finish(f.read(CHUNK_SIZE), cursor, commit)
                return

            while f.tell() < file_size:
                if (file_size - f.tell()) <= CHUNK_SIZE:
                    self.conn.files_upload_session_finish(f.read(CHUNK_SIZE), cursor, commit)
                else:
                    self.conn.files_upload_session_append_v2(f.read(CHUNK_SIZE), cursor)
                    cursor.offset = f.tell()

    def _exists_and_is_dir(self, path):
        """
        Auxiliary method, used by the 'accept_trailing_slash' and 'accept_trailing_slash_in_existing_dirpaths' decorators
        :param path: a Dropbox path that does NOT ends with a '/' (even if it is a directory)
        """
        if path == '/':
            return True
        try:
            md = self.conn.files_get_metadata(path)
            is_dir = isinstance(md, dropbox.files.FolderMetadata)
            return is_dir
        except dropbox.exceptions.ApiError:
            return False


class ReadableDropboxFile:
    def __init__(self, path, client):
        """
        Represents a file inside the Dropbox cloud which will be read

        :param str path: Dropbpx path of the file to be read (always starting with /)
        :param DropboxClient client: a DropboxClient object (initialized with a valid token)

        """
        self.path = path
        self.client = client
        self.download_file_location = os.path.join(tempfile.mkdtemp(prefix=str(time.time())),
                                                   ntpath.basename(path))
        self.fid = None
        self.closed = False

    def read(self):
        return self.client.download_as_bytes(self.path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def __del__(self):
        self.close()
        if os.path.exists(self.download_file_location):
            os.remove(self.download_file_location)

    def close(self):
        self.closed = True

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return False


class AtomicWritableDropboxFile(AtomicLocalFile):
    def __init__(self, path, client):
        """
        Represents a file that will be created inside the Dropbox cloud

        :param str path: Destination path inside Dropbox
        :param DropboxClient client: a DropboxClient object (initialized with a valid token, for the desired account)
        """
        super(AtomicWritableDropboxFile, self).__init__(path)
        self.path = path
        self.client = client

    def move_to_final_destination(self):
        """
        After editing the file locally, this function uploads it to the Dropbox cloud
        """
        self.client.upload(self.tmp_path, self.path)


class DropboxTarget(FileSystemTarget):
    """
    A Dropbox filesystem target.
    """

    def __init__(self, path, token, format=None, user_agent="Luigi"):
        """
        Create an Dropbox Target for storing data in a dropbox.com account

        **About the path parameter**

        The path must start with '/' and should not end with '/' (even if it is a directory).
        The path must not contain adjacent slashes ('/files//img.jpg' is an invalid path)

        If the app has 'App folder' access, then / will refer to this app folder (which
        mean that there is no need to prepend the name of the app to the path)
        Otherwise, if the app has 'full access', then / will refer to the root of the Dropbox folder


        **About the token parameter:**

        The Dropbox target requires a valid OAuth2 token as a parameter (which means that a `Dropbox API app
        <https://www.dropbox.com/developers/apps>`_ must be created. This app can have 'App folder' access
        or 'Full Dropbox', as desired).

        Information about generating the token can be read here:

        - https://dropbox-sdk-python.readthedocs.io/en/latest/api/oauth.html#dropbox.oauth.DropboxOAuth2Flow
        - https://blogs.dropbox.com/developers/2014/05/generate-an-access-token-for-your-own-account/

        :param str path: Remote path in Dropbox (starting with '/').
        :param str token: a valid OAuth2 Dropbox token.
        :param luigi.Format format: the luigi format to use (e.g. `luigi.format.Nop`)


        """
        super(DropboxTarget, self).__init__(path)

        if not token:
            raise ValueError("The token parameter must contain a valid Dropbox Oauth2 Token")

        self.path = path
        self.token = token
        self.client = DropboxClient(token, user_agent)
        self.format = format or luigi.format.get_default_format()

    @property
    def fs(self):
        return self.client

    @contextmanager
    def temporary_path(self):
        tmp_dir = tempfile.mkdtemp()
        num = random.randrange(0, 1e10)
        temp_path = '{}{}luigi-tmp-{:010}{}'.format(
            tmp_dir, os.sep,
            num, ntpath.basename(self.path))

        yield temp_path
        # We won't reach here if there was an user exception.
        self.fs.upload(temp_path, self.path)

    def open(self, mode):
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '%s'" % mode)
        if mode == 'r':
            return self.format.pipe_reader(ReadableDropboxFile(self.path, self.client))
        else:
            return self.format.pipe_writer(AtomicWritableDropboxFile(self.path, self.client))
