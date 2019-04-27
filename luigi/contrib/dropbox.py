from __future__ import absolute_import

import datetime
import logging
import ntpath
import os
import random
import tempfile
from contextlib import contextmanager

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


class DropboxClient(FileSystem):
    """
    Create an Azure Blob Storage client for authentication.

    see :py:meth:`luigi.contrib.dropbox:DropboxTarget:__init__` for more information about how to
    generate the token.
    """

    def __init__(self, token):

        if not token:
            raise ValueError("The token parameter must contain a valid Dropbox Oauth2 Token")

        try:
            conn = dropbox.dropbox.Dropbox(oauth2_access_token=token, user_agent="Luigi")
        except Exception as e:
            raise Exception("Cannot connect to the Dropbox storage. Check the connection and the token. \n" + repr(e))

        self.token = token
        self.conn = conn

    def exists(self, path):
        if path == '/':
            return True
        try:
            md = self.conn.files_get_metadata(path)
            return bool(md)
        except dropbox.exceptions.ApiError as e:
            if isinstance(e.error.get_path(), dropbox.files.LookupError):
                return False
            else:
                raise e

    def remove(self, path, recursive=True, skip_trash=True):
        if not self.exists(path):
            return False
        self.conn.files_delete_v2(path)
        return True

    def mkdir(self, path, parents=True, raise_if_exists=False):
        if self.exists(path):
            if not self.isdir(path):
                raise luigi.target.NotADirectory()
            elif raise_if_exists:
                raise luigi.target.FileAlreadyExists()
            else:
                return

        self.conn.files_create_folder_v2(path)

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

    def listdir(self, path, **kwargs):
        dirs = []
        lister = self.conn.files_list_folder(path, recursive=True, **kwargs)
        dirs.extend(lister.entries)
        while lister.has_more:
            lister = self.conn.files_list_folder_continue(lister.cursor)
            dirs.extend(lister.entries)
        return [d.path_display for d in dirs]

    def move(self, path, dest):
        self.conn.files_move_v2(from_path=path, to_path=dest)

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


class ReadableDropboxFile(object):
    def __init__(self, path, client):
        """
        :param str path: adfbool
        :param DropboxClient client: asdfa

        """
        self.path = path
        self.client = client
        self.download_file_location = os.path.join(tempfile.mkdtemp(prefix=str(datetime.datetime.utcnow())),
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
        if os._exists(self.download_file_location):
            os.remove(self.download_file_location)

    def close(self):
        self.closed = True

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return False

    def seek(self, offset, whence=None):
        pass


class AtomicWritableDropboxFile(AtomicLocalFile):
    def __init__(self, path, client):
        super(AtomicWritableDropboxFile, self).__init__(path)
        self.path = path
        self.client = client

    def move_to_final_destination(self):
        self.client.upload(self.tmp_path, self.path)


class DropboxTarget(FileSystemTarget):
    """
    A Dropbox filesystem target.
    """

    def __init__(self, path, token=None, format=None):
        """
        Constructor for the Dropbox filesystem target.

        The Dropbox target requires an OAuth2 token as a parameter (which means that a `Dropbox API app
        <https://www.dropbox.com/developers/apps>`_ must be created. This app can have 'App folder' access
        or 'Full Dropbox', as desired).

        More infornation about generating the token can be read here:

        - https://dropbox-sdk-python.readthedocs.io/en/latest/api/oauth.html#dropbox.oauth.DropboxOAuth2Flow
        - https://blogs.dropbox.com/developers/2014/05/generate-an-access-token-for-your-own-account/

        The path must start with '/'. If the app has 'App folder' access, then / will refer to this app folder (which
        mean that there is no need to prepend the name of the app to the path)


        :param str path: Remote path in Dropbox (starting with '/')
        :param str token: A OAuth2 token for the Dropbox account
        :param format: the luigi format to use (e.g. luigi.format.Nop)


        """
        super(DropboxTarget, self).__init__(path)

        if not token:
            raise ValueError("The token parameter must contain a valid Dropbox Oauth2 Token")

        self.path = path
        self.token = token
        self.client = DropboxClient(token)
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
