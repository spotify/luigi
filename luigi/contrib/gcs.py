# -*- coding: utf-8 -*-
#
# Copyright 2015 Twitter Inc
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

"""luigi bindings for Google Cloud Storage"""

import io
import logging
import mimetypes
import os
import tempfile
import time
from urllib.parse import urlsplit
from io import BytesIO

from luigi.contrib import gcp
import luigi.target
from luigi.format import FileWrapper

logger = logging.getLogger('luigi-interface')

try:
    import httplib2

    from googleapiclient import errors
    from googleapiclient import discovery
    from googleapiclient import http
except ImportError:
    logger.warning("Loading GCS module without the python packages googleapiclient & google-auth. \
        This will crash at runtime if GCS functionality is used.")
else:
    # Retry transport and file IO errors.
    RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)

# Number of times to retry failed downloads.
NUM_RETRIES = 5

# Number of bytes to send/receive in each request.
CHUNKSIZE = 10 * 1024 * 1024

# Mimetype to use if one can't be guessed from the file extension.
DEFAULT_MIMETYPE = 'application/octet-stream'

# Time to sleep while waiting for eventual consistency to finish.
EVENTUAL_CONSISTENCY_SLEEP_INTERVAL = 0.1

# Maximum number of sleeps for eventual consistency.
EVENTUAL_CONSISTENCY_MAX_SLEEPS = 300


def _wait_for_consistency(checker):
    """Eventual consistency: wait until GCS reports something is true.

    This is necessary for e.g. create/delete where the operation might return,
    but won't be reflected for a bit.
    """
    for _ in range(EVENTUAL_CONSISTENCY_MAX_SLEEPS):
        if checker():
            return

        time.sleep(EVENTUAL_CONSISTENCY_SLEEP_INTERVAL)

    logger.warning('Exceeded wait for eventual GCS consistency - this may be a'
                   'bug in the library or something is terribly wrong.')


class InvalidDeleteException(luigi.target.FileSystemException):
    pass


class GCSClient(luigi.target.FileSystem):
    """An implementation of a FileSystem over Google Cloud Storage.

       There are several ways to use this class. By default it will use the app
       default credentials, as described at https://developers.google.com/identity/protocols/application-default-credentials .
       Alternatively, you may pass an google-auth credentials object. e.g. to use a service account::

         credentials = google.auth.jwt.Credentials.from_service_account_info(
             '012345678912-ThisIsARandomServiceAccountEmail@developer.gserviceaccount.com',
             'These are the contents of the p12 file that came with the service account',
             scope='https://www.googleapis.com/auth/devstorage.read_write')
         client = GCSClient(oauth_credentials=credentails)

        The chunksize parameter specifies how much data to transfer when downloading
        or uploading files.

    .. warning::
      By default this class will use "automated service discovery" which will require
      a connection to the web. The google api client downloads a JSON file to "create" the
      library interface on the fly. If you want a more hermetic build, you can pass the
      contents of this file (currently found at https://www.googleapis.com/discovery/v1/apis/storage/v1/rest )
      as the ``descriptor`` argument.
    """
    def __init__(self, oauth_credentials=None, descriptor='', http_=None,
                 chunksize=CHUNKSIZE, **discovery_build_kwargs):
        self.chunksize = chunksize
        authenticate_kwargs = gcp.get_authenticate_kwargs(oauth_credentials, http_)

        build_kwargs = authenticate_kwargs.copy()
        build_kwargs.update(discovery_build_kwargs)

        if descriptor:
            self.client = discovery.build_from_document(descriptor, **build_kwargs)
        else:
            build_kwargs.setdefault('cache_discovery', False)
            self.client = discovery.build('storage', 'v1', **build_kwargs)

    def _path_to_bucket_and_key(self, path):
        (scheme, netloc, path, _, _) = urlsplit(path)
        assert scheme == 'gs'
        path_without_initial_slash = path[1:]
        return netloc, path_without_initial_slash

    def _is_root(self, key):
        return len(key) == 0 or key == '/'

    def _add_path_delimiter(self, key):
        return key if key[-1:] == '/' else key + '/'

    def _obj_exists(self, bucket, obj):
        try:
            self.client.objects().get(bucket=bucket, object=obj).execute()
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise
        else:
            return True

    def _list_iter(self, bucket, prefix):
        request = self.client.objects().list(bucket=bucket, prefix=prefix)
        response = request.execute()

        while response is not None:
            for it in response.get('items', []):
                yield it

            request = self.client.objects().list_next(request, response)
            if request is None:
                break

            response = request.execute()

    def _do_put(self, media, dest_path):
        bucket, obj = self._path_to_bucket_and_key(dest_path)

        request = self.client.objects().insert(bucket=bucket, name=obj, media_body=media)
        if not media.resumable():
            return request.execute()

        response = None
        attempts = 0
        while response is None:
            error = None
            try:
                status, response = request.next_chunk()
                if status:
                    logger.debug('Upload progress: %.2f%%', 100 * status.progress())
            except errors.HttpError as err:
                error = err
                if err.resp.status < 500:
                    raise
                logger.warning('Caught error while uploading', exc_info=True)
            except RETRYABLE_ERRORS as err:
                logger.warning('Caught error while uploading', exc_info=True)
                error = err

            if error:
                attempts += 1
                if attempts >= NUM_RETRIES:
                    raise error
            else:
                attempts = 0

        _wait_for_consistency(lambda: self._obj_exists(bucket, obj))
        return response

    def exists(self, path):
        bucket, obj = self._path_to_bucket_and_key(path)
        if self._obj_exists(bucket, obj):
            return True

        return self.isdir(path)

    def isdir(self, path):
        bucket, obj = self._path_to_bucket_and_key(path)
        if self._is_root(obj):
            try:
                self.client.buckets().get(bucket=bucket).execute()
            except errors.HttpError as ex:
                if ex.resp['status'] == '404':
                    return False
                raise

        obj = self._add_path_delimiter(obj)
        if self._obj_exists(bucket, obj):
            return True

        # Any objects with this prefix
        resp = self.client.objects().list(bucket=bucket, prefix=obj, maxResults=20).execute()
        lst = next(iter(resp.get('items', [])), None)
        return bool(lst)

    def remove(self, path, recursive=True):
        (bucket, obj) = self._path_to_bucket_and_key(path)

        if self._is_root(obj):
            raise InvalidDeleteException(
                'Cannot delete root of bucket at path {}'.format(path))

        if self._obj_exists(bucket, obj):
            self.client.objects().delete(bucket=bucket, object=obj).execute()
            _wait_for_consistency(lambda: not self._obj_exists(bucket, obj))
            return True

        if self.isdir(path):
            if not recursive:
                raise InvalidDeleteException(
                    'Path {} is a directory. Must use recursive delete'.format(path))

            req = http.BatchHttpRequest()
            for it in self._list_iter(bucket, self._add_path_delimiter(obj)):
                req.add(self.client.objects().delete(bucket=bucket, object=it['name']))
            req.execute()

            _wait_for_consistency(lambda: not self.isdir(path))
            return True

        return False

    def put(self, filename, dest_path, mimetype=None, chunksize=None):
        chunksize = chunksize or self.chunksize
        resumable = os.path.getsize(filename) > 0

        mimetype = mimetype or mimetypes.guess_type(dest_path)[0] or DEFAULT_MIMETYPE
        media = http.MediaFileUpload(filename, mimetype=mimetype, chunksize=chunksize, resumable=resumable)

        self._do_put(media, dest_path)

    def _forward_args_to_put(self, kwargs):
        return self.put(**kwargs)

    def put_multiple(self, filepaths, remote_directory, mimetype=None, chunksize=None, num_process=1):
        if isinstance(filepaths, str):
            raise ValueError(
                'filenames must be a list of strings. If you want to put a single file, '
                'use the `put(self, filename, ...)` method'
            )

        put_kwargs_list = [
            {
                'filename': filepath,
                'dest_path': os.path.join(remote_directory, os.path.basename(filepath)),
                'mimetype': mimetype,
                'chunksize': chunksize,
            }
            for filepath in filepaths
        ]

        if num_process > 1:
            from multiprocessing import Pool
            from contextlib import closing
            with closing(Pool(num_process)) as p:
                return p.map(self._forward_args_to_put, put_kwargs_list)
        else:
            for put_kwargs in put_kwargs_list:
                self._forward_args_to_put(put_kwargs)

    def put_string(self, contents, dest_path, mimetype=None):
        mimetype = mimetype or mimetypes.guess_type(dest_path)[0] or DEFAULT_MIMETYPE
        assert isinstance(mimetype, str)
        if not isinstance(contents, bytes):
            contents = contents.encode("utf-8")
        media = http.MediaIoBaseUpload(BytesIO(contents), mimetype, resumable=bool(contents))
        self._do_put(media, dest_path)

    def mkdir(self, path, parents=True, raise_if_exists=False):
        if self.exists(path):
            if raise_if_exists:
                raise luigi.target.FileAlreadyExists()
            elif not self.isdir(path):
                raise luigi.target.NotADirectory()
            else:
                return

        self.put_string(b"", self._add_path_delimiter(path), mimetype='text/plain')

    def copy(self, source_path, destination_path):
        src_bucket, src_obj = self._path_to_bucket_and_key(source_path)
        dest_bucket, dest_obj = self._path_to_bucket_and_key(destination_path)

        if self.isdir(source_path):
            src_prefix = self._add_path_delimiter(src_obj)
            dest_prefix = self._add_path_delimiter(dest_obj)

            source_path = self._add_path_delimiter(source_path)
            copied_objs = []
            for obj in self.listdir(source_path):
                suffix = obj[len(source_path):]

                self.client.objects().copy(
                    sourceBucket=src_bucket,
                    sourceObject=src_prefix + suffix,
                    destinationBucket=dest_bucket,
                    destinationObject=dest_prefix + suffix,
                    body={}).execute()
                copied_objs.append(dest_prefix + suffix)

            _wait_for_consistency(
                lambda: all(self._obj_exists(dest_bucket, obj)
                            for obj in copied_objs))
        else:
            self.client.objects().copy(
                sourceBucket=src_bucket,
                sourceObject=src_obj,
                destinationBucket=dest_bucket,
                destinationObject=dest_obj,
                body={}).execute()
            _wait_for_consistency(lambda: self._obj_exists(dest_bucket, dest_obj))

    def rename(self, *args, **kwargs):
        """
        Alias for ``move()``
        """
        self.move(*args, **kwargs)

    def move(self, source_path, destination_path):
        """
        Rename/move an object from one GCS location to another.
        """
        self.copy(source_path, destination_path)
        self.remove(source_path)

    def listdir(self, path):
        """
        Get an iterable with GCS folder contents.
        Iterable contains paths relative to queried path.
        """
        bucket, obj = self._path_to_bucket_and_key(path)

        obj_prefix = self._add_path_delimiter(obj)
        if self._is_root(obj_prefix):
            obj_prefix = ''

        obj_prefix_len = len(obj_prefix)
        for it in self._list_iter(bucket, obj_prefix):
            yield self._add_path_delimiter(path) + it['name'][obj_prefix_len:]

    def list_wildcard(self, wildcard_path):
        """Yields full object URIs matching the given wildcard.

        Currently only the '*' wildcard after the last path delimiter is supported.

        (If we need "full" wildcard functionality we should bring in gsutil dependency with its
        https://github.com/GoogleCloudPlatform/gsutil/blob/master/gslib/wildcard_iterator.py...)
        """
        path, wildcard_obj = wildcard_path.rsplit('/', 1)
        assert '*' not in path, "The '*' wildcard character is only supported after the last '/'"
        wildcard_parts = wildcard_obj.split('*')
        assert len(wildcard_parts) == 2, "Only one '*' wildcard is supported"

        for it in self.listdir(path):
            if it.startswith(path + '/' + wildcard_parts[0]) and it.endswith(wildcard_parts[1]) and \
                   len(it) >= len(path + '/' + wildcard_parts[0]) + len(wildcard_parts[1]):
                yield it

    def download(self, path, chunksize=None, chunk_callback=lambda _: False):
        """Downloads the object contents to local file system.

        Optionally stops after the first chunk for which chunk_callback returns True.
        """
        chunksize = chunksize or self.chunksize
        bucket, obj = self._path_to_bucket_and_key(path)

        with tempfile.NamedTemporaryFile(delete=False) as fp:
            # We can't return the tempfile reference because of a bug in python: http://bugs.python.org/issue18879
            return_fp = _DeleteOnCloseFile(fp.name, 'r')

            # Special case empty files because chunk-based downloading doesn't work.
            result = self.client.objects().get(bucket=bucket, object=obj).execute()
            if int(result['size']) == 0:
                return return_fp

            request = self.client.objects().get_media(bucket=bucket, object=obj)
            downloader = http.MediaIoBaseDownload(fp, request, chunksize=chunksize)

            attempts = 0
            done = False
            while not done:
                error = None
                try:
                    _, done = downloader.next_chunk()
                    if chunk_callback(fp):
                        done = True
                except errors.HttpError as err:
                    error = err
                    if err.resp.status < 500:
                        raise
                    logger.warning('Error downloading file, retrying', exc_info=True)
                except RETRYABLE_ERRORS as err:
                    logger.warning('Error downloading file, retrying', exc_info=True)
                    error = err

                if error:
                    attempts += 1
                    if attempts >= NUM_RETRIES:
                        raise error
                else:
                    attempts = 0

        return return_fp


class _DeleteOnCloseFile(io.FileIO):
    def close(self):
        super(_DeleteOnCloseFile, self).close()
        try:
            os.remove(self.name)
        except OSError:
            # Catch a potential threading race condition and also allow this
            # method to be called multiple times.
            pass

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return True


class AtomicGCSFile(luigi.target.AtomicLocalFile):
    """
    A GCS file that writes to a temp file and put to GCS on close.
    """

    def __init__(self, path, gcs_client):
        self.gcs_client = gcs_client
        super(AtomicGCSFile, self).__init__(path)

    def move_to_final_destination(self):
        self.gcs_client.put(self.tmp_path, self.path)


class GCSTarget(luigi.target.FileSystemTarget):
    fs = None

    def __init__(self, path, format=None, client=None):
        super(GCSTarget, self).__init__(path)
        if format is None:
            format = luigi.format.get_default_format()

        self.format = format
        self.fs = client or GCSClient()

    def open(self, mode='r'):
        if mode == 'r':
            return self.format.pipe_reader(
                FileWrapper(io.BufferedReader(self.fs.download(self.path))))
        elif mode == 'w':
            return self.format.pipe_writer(AtomicGCSFile(self.path, self.fs))
        else:
            raise ValueError("Unsupported open mode '{}'".format(mode))


class GCSFlagTarget(GCSTarget):
    """
    Defines a target directory with a flag-file (defaults to `_SUCCESS`) used
    to signify job success.

    This checks for two things:

    * the path exists (just like the GCSTarget)
    * the _SUCCESS file exists within the directory.

    Because Hadoop outputs into a directory and not a single file,
    the path is assumed to be a directory.

    This is meant to be a handy alternative to AtomicGCSFile.

    The AtomicFile approach can be burdensome for GCS since there are no directories, per se.

    If we have 1,000,000 output files, then we have to rename 1,000,000 objects.
    """

    fs = None

    def __init__(self, path, format=None, client=None, flag='_SUCCESS'):
        """
        Initializes a GCSFlagTarget.

        :param path: the directory where the files are stored.
        :type path: str
        :param client:
        :type client:
        :param flag:
        :type flag: str
        """
        if format is None:
            format = luigi.format.get_default_format()

        if path[-1] != "/":
            raise ValueError("GCSFlagTarget requires the path to be to a "
                             "directory.  It must end with a slash ( / ).")
        super(GCSFlagTarget, self).__init__(path, format=format, client=client)
        self.format = format
        self.fs = client or GCSClient()
        self.flag = flag

    def exists(self):
        flag_target = self.path + self.flag
        return self.fs.exists(flag_target)
